(ns hitchhiker.tree.bootstrap.konserve
  (:refer-clojure :exclude [subvec])
  (:require
    [clojure.core.rrb-vector :refer [catvec subvec]]
    [konserve.cache :as k]
    [konserve.memory :refer [new-mem-store]]
    [konserve.protocols :as kp]
    [hasch.core :as h]
    [clojure.set :as set]
    [hitchhiker.tree.messaging :as msg]
    [hitchhiker.tree :as tree]
    [hitchhiker.tree.node :as n]
    [hitchhiker.tree.backend :as b]
    [hitchhiker.tree.key-compare :as c]
    [hitchhiker.tree.utils.async :as ha]
    [hitchhiker.tree.codec.nippy :as nippy]
    #?@(:clj  [[clojure.core.async :as async]
               [clojure.core.cache :as cache]]
        :cljs [[cljs.core.async :include-macros true :as async]
               [cljs.cache :as cache]])
    [hitchhiker.tree :as core]))

(declare encode)

(defn nilify
  [m ks]
  (reduce (fn [m k] (assoc m k nil))
          m
          ks))

(defn encode-index-node
  [node]
  (-> node
      (nilify [:storage-addr :*last-key-cache :op-buf :ops-storage-addr])
      (assoc :children (mapv encode (:children node)))))

(defn encode-data-node
  [node]
  (nilify node
          [:storage-addr
           :*last-key-cache]))

(defn encode-address
  [node]
  (nilify node
          [:store
           :storage-addr]))

(defn encode
  [node]
  (cond
    (tree/index-node? node) (encode-index-node node)
    (tree/data-node? node) (encode-data-node node)
    (n/address? node) (encode-address node)
    :else node))

(defn synthesize-storage-address
  "Given a key, returns a promise containing that key for use as a storage-addr"
  [key]
  (ha/promise-chan key))

(defrecord KonserveOpsAddr [store konserve-key]
  n/IAddress
  (-dirty? [_] false)
  (-dirty! [this] this)
  (-ops-dirty? [_] false)
  (-ops-dirty! [this] this)

  (-resolve-chan [_]
    (ha/go-try
      ; bypass the cache, because it is based off the first key in keyvec
      (let [ch (kp/-get-in store [:ops konserve-key])
            result (ha/if-async?
                     (ha/<? ch)
                     (async/<!! ch))]
        result))))

(defn konserve-ops-addr
  [store konserve-key]
  (->KonserveOpsAddr store konserve-key))

(defrecord KonserveAddr [store last-key konserve-key storage-addr]
  n/INode
  (-last-key [_] last-key)

  n/IAddress
  (-dirty? [_] false)
  (-dirty! [this] this)
  (-ops-dirty? [_] false)
  (-ops-dirty! [this] this)

  (-resolve-chan [this]
    (ha/go-try
      (let [cache (:cache store)
            node (if-let [v (cache/lookup @cache konserve-key)]
                   (do
                     (swap! cache cache/hit konserve-key)
                     (assoc v :storage-addr (synthesize-storage-address this)))
                   (let [ch (k/get-in store [konserve-key])]
                     (assoc (ha/if-async?
                              (ha/<? ch)
                              (async/<!! ch))
                            :storage-addr (synthesize-storage-address this))))
            node (if (tree/index-node? node)
                   (let [ops-addr (konserve-ops-addr store konserve-key)]
                     (if-let [op-buf (some-> ops-addr
                                             (n/-resolve-chan)
                                             (ha/<?))]
                       (assoc node :op-buf op-buf
                                   :ops-storage-addr (synthesize-storage-address ops-addr))
                       (update node :op-buf #(or % []))))
                   node)]
        node))))

(defn konserve-addr
  [store last-key konserve-key]
  (->KonserveAddr store
                  last-key
                  konserve-key
                  (ha/promise-chan konserve-key)))

(defrecord KonserveBackend [store]
  b/IBackend
  (-new-session [_] (atom {:writes 0 :deletes 0}))
  (-anchor-root [_ {:keys [konserve-key] :as node}]
    node)
  (-write-node [this node session]
    (ha/go-try
      (swap! session update-in [:writes] inc)
      (let [pnode (encode node)
            id (h/uuid pnode)
            ch (k/assoc-in store [id] pnode)
            addr (konserve-addr store (n/-last-key node) id)
            op-ch (when (core/index-node? node)
                    (b/-write-ops-buffer this addr (:op-buf node) session))]
        (ha/<? ch)
        [addr (some-> op-ch (ha/<?))])))
  (-write-ops-buffer [_ node-address ops-buffer session]
    (ha/go-try
      (swap! session update :writes inc)
      (let [buffer (into [] ops-buffer)
            id (or (:konserve-key node-address) node-address)
            ; bypass the cache because it does dumb things with the first key in keyvec
            ch (kp/-assoc-in store [:ops id] buffer)]
        (ha/<? ch)
        (konserve-ops-addr store id))))
  (-delete-addr [_ addr session]
    (swap! session update :deletes inc)))

(defn get-root-key
  [tree]
  (or
   (-> tree :storage-addr (async/poll!) :konserve-key)
   (-> tree :storage-addr (async/poll!))))

(defn create-tree-from-root-key
  [store root-key]
  (ha/go-try
   (let [ch (k/get-in store [root-key])
         val (ha/if-async?
              (ha/<? ch)
              (async/<!! ch))
         ;; need last key to bootstrap
         last-key (n/-last-key (assoc val :storage-addr (synthesize-storage-address root-key)))]
     (ha/<? (n/-resolve-chan (konserve-addr store
                                            last-key
                                            root-key))))))

(defn add-hitchhiker-tree-handlers [store]
  (nippy/ensure-installed!)
  (swap! (:read-handlers store)
         merge
         {'hitchhiker.tree.bootstrap.konserve.KonserveAddr
          (fn [{:keys [last-key konserve-key]}]
            (konserve-addr store
                           last-key
                           konserve-key))
          'hitchhiker.tree.DataNode
          (fn [{:keys [children cfg] :as d}]
            (tree/data-node (into (sorted-map-by c/-compare)
                                  children)
                            cfg))
          'hitchhiker.tree.IndexNode
          (fn [{:keys [children cfg op-buf]}]
            (tree/index-node (vec children)
                             (vec op-buf)
                             cfg))
          'hitchhiker.tree.messaging.InsertOp
          msg/map->InsertOp
          'hitchhiker.tree.messaging.DeleteOp
          msg/map->DeleteOp
          'hitchhiker.tree.Config
          tree/map->Config
          'hitchhiker.tree.bootstrap.konserve.KonserveOpsAddr
          (fn [{:keys [konserve-key]}]
            (->KonserveOpsAddr store konserve-key))})
  (swap! (:write-handlers store)
         merge
         {'hitchhiker.tree.bootstrap.konserve.KonserveAddr
          encode-address
          'hitchhiker.tree.DataNode
          encode-data-node
          'hitchhiker.tree.IndexNode
          encode-index-node
          'hitchhiker.tree.bootstrap.konserve.KonserveOpsAddr
          (fn [addr] (dissoc addr :store))})
  store)
