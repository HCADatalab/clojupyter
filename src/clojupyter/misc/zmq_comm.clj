(ns clojupyter.misc.zmq-comm
  (:require [clojure.pprint :as pp]
            [taoensso.timbre :as log]
            [zeromq.zmq :as zmq]))

(defn parts-to-message [parts]
  (let [delim "<IDS|MSG>"
        delim-bytes (.getBytes delim "UTF-8")
        [idents [_ & more-parts]] (split-with #(not (java.util.Arrays/equals delim-bytes ^bytes %)) parts)
        blobs (map #(new String % "UTF-8") more-parts)
        blob-names [:signature :header :parent-header :metadata :content]
        message (merge
                 {:idents idents :delimiter delim}
                 (zipmap blob-names blobs)
                 {:buffers (drop (count blob-names) blobs)})]
    message))

(defn zmq-read-raw-message [socket]
  (parts-to-message (zmq/receive-all socket)))

