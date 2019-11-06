(ns hitchhiker.tree.backend)

(defprotocol IBackend
  (-new-session [backend] "Returns a session object that will collect stats")
  (-write-node [backend node session]
    "Writes the given node to storage, returning a go-block with its assigned address
    and ops buffer address, as a pair.")
  (-write-ops-buffer [backend node-address ops-buffer session]
    "Writes the given ops buffer to storage, returning a channel with its assigned address.
    node-address is the storage address of the node containing this ops buffer.")
  (-anchor-root [backend node] "Tells the backend this is a temporary root")
  (-delete-addr [backend addr session] "Deletes the given addr from storage"))
