(ns hitchhiker.tree.node.testing
  (:require
   [hitchhiker.tree.utils.async :as ha]
   [hitchhiker.tree.node :as n]
   [hitchhiker.tree :as tree]))

(defrecord TestingAddr [last-key node resolve-ch]
  n/IAddress
  (-dirty? [this] false)
  (-dirty! [this] this)
  (-ops-dirty? [this] false)
  (-ops-dirty! [this] this)
  (-resolve-chan [_] resolve-ch)
  n/INode
  (-last-key [_] last-key))

(defn testing-addr
  [last-key node]
  (->TestingAddr last-key
                 node
                 (ha/go-try node)))
