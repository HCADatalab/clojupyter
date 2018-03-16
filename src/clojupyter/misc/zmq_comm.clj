(ns clojupyter.misc.zmq-comm
  (:require [clojure.pprint :as pp]
            [taoensso.timbre :as log]
            [zeromq.zmq :as zmq]))

(defn- string-to-bytes [s]
  (. (. (java.nio.charset.Charset/forName "UTF-8") encode s) array))

(defn parts-to-message [parts]
  (let [delim "<IDS|MSG>"
        delim-byte (string-to-bytes delim)
        delim-idx (first
                   (map first (filter #(apply = (map seq [(second %) delim-byte]))
                                      (map-indexed vector parts))))
        idents (take delim-idx parts)
        blobs (map #(new String % "UTF-8")
                   (drop (inc delim-idx) parts))
        blob-names [:signature :header :parent-header :metadata :content]
        n-blobs (count blob-names)
        message (merge
                 {:idents idents :delimiter delim}
                 (zipmap blob-names (take n-blobs blobs))
                 {:buffers (drop n-blobs blobs)})]
    message))

(defn zmq-send
  ([sockets socket message]
    (apply zmq/send [(get sockets socket) message]))
  ([sockets socket message zmq-flag]
    (apply zmq/send [(get sockets socket) message zmq-flag])))

(defn zmq-read-raw-message [sockets socket flag]
  (let [recv-all (fn [socket flag]
                   (loop [acc (transient [])]
                     (if-let [part (zmq/receive socket flag)]
                       (let [new-acc (conj! acc part)]
                         (if (zmq/receive-more? socket)
                           (recur new-acc)
                           (persistent! new-acc)))
                       nil)))]
    (if-let [parts (recv-all (get sockets socket) flag)]
      (let [message (parts-to-message parts)]
        (log/info "Receive message\n" (with-out-str (pp/pprint message)))
        message)
      nil)))

(defn zmq-recv [sockets socket]
  (zmq/receive (get sockets socket)))
