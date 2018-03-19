(ns clojupyter.core
  (:require [beckon]
            [clojupyter.middleware.mime-values]
            [clojupyter.misc.zmq-comm :as zmqc]
            [clojupyter.misc.unrepl-comm :as unrepl-comm]
            [clojupyter.misc.messages :refer :all]
            [clojure.data.json :as json]
            [clojure.pprint :as pp]
            [clojure.stacktrace :as st]
            [clojure.tools.nrepl :as nrepl]
            [clojure.tools.nrepl.server :as nrepl.server]
            [clojure.walk :as walk]
            [clojure.core.async :as a]
            [taoensso.timbre :as log]
            [zeromq.zmq :as zmq])
  (:import [java.net ServerSocket])
  (:gen-class :main true))

(defn get-free-port!
  "Get a free port. Problem?: might be taken before I use it."
  []
  (let [socket (ServerSocket. 0)
        port (.getLocalPort socket)]
    (.close socket)
    port))

(defn prep-config [args]
  (-> args
      first
      slurp
      json/read-str
      walk/keywordize-keys))

(defn address [config service]
  (str (:transport config) "://" (:ip config) ":" (service config)))

(def clojupyter-middleware
  '[clojupyter.middleware.mime-values/mime-values])

(defn clojupyer-nrepl-handler []
  ;; dynamically load to allow cider-jack-in to work
  ;; see https://github.com/clojure-emacs/cider-nrepl/issues/447
  (require 'cider.nrepl)
  (apply nrepl.server/default-handler
         (map resolve
              (concat (ns-resolve 'cider.nrepl 'cider-nrepl-middleware)
                      clojupyter-middleware))))

(defn start-nrepl-server []
  (nrepl.server/start-server
   :port (get-free-port!)
   :handler (clojupyer-nrepl-handler)))

(defn exception-handler [e]
  (log/error (with-out-str (st/print-stack-trace e 20))))

(defn configure-shell-handler [alive sockets nrepl-comm socket key]
  (let [execute-request (execute-request-handler alive sockets nrepl-comm socket)]
    (fn [message]
      (let [msg-type (get-in message [:header :msg_type])]
        (case msg-type
          "execute_request"     (execute-request   message key)
          "kernel_info_request" (kernel-info-reply sockets
                                                   socket message key)
          "history_request"     (history-reply     sockets
                                                   socket message key)
          "shutdown_request"    (shutdown-reply    alive sockets nrepl-comm
                                                   socket message key)
          "comm_info_request"   (comm-info-reply   sockets
                                                   socket message key)
          "comm_msg"            (comm-msg-reply    sockets
                                                   socket message key)
          "is_complete_request" (is-complete-reply sockets
                                                   socket message key)
          "complete_request"    (complete-reply    sockets nrepl-comm
                                                   socket message key)
          "comm_open"           (comm-open-reply   sockets
                                                   socket message key)
          (do
            (log/error "Message type" msg-type "not handled yet. Exiting.")
            (log/error "Message dump:" message)
            (System/exit -1)))))))

(defn configure-control-handler [alive sockets nrepl-comm socket key]
  (fn [message]
    (let [msg-type (get-in message [:header :msg_type])]
      (case msg-type
        "kernel_info_request" (kernel-info-reply sockets
                                                 socket message key)
        "shutdown_request"    (shutdown-reply    alive sockets nrepl-comm
                                                 socket message key)
        (do
          (log/error "Message type" msg-type "not handled yet. Exiting.")
          (log/error "Message dump:" message)
          (System/exit -1))))))

(defn process-event [alive sockets socket key handler]
  (let [message        (zmqc/zmq-read-raw-message sockets socket 0)
        parsed-message (parse-message message)
        parent-header  (:header parsed-message)
        session-id     (:session parent-header)]
    (send-message (:iopub-socket sockets) "status"
                  (status-content "busy") parent-header session-id {} key)
    (handler parsed-message)
    (send-message (:iopub-socket sockets) "status"
                  (status-content "idle") parent-header session-id {} key)))

(defn event-loop [alive sockets socket key handler]
  (a/thread
    (try
     (while @alive
       (process-event alive sockets socket key handler))
     (catch Exception e
       (exception-handler e)))))

(defn heartbeat-loop [alive hb-socket]
  (a/thread
    (try
      (while @alive
        (zmq/send hb-socket (zmq/receive hb-socket)))
      (catch Exception e
        (exception-handler e)))))

(defn shell-loop [alive sockets nrepl-comm key]
  (let [socket        :shell-socket
        shell-handler (configure-shell-handler alive sockets nrepl-comm socket key)]
    (event-loop alive sockets socket key shell-handler)))

(defn control-loop [alive sockets nrepl-comm key]
  (let [socket          :control-socket
        control-handler (configure-control-handler alive sockets nrepl-comm socket key)]
    (event-loop alive sockets socket key control-handler)))

(defn- with-unrepl-comm [f]
  (let [unrepl-comm    (unrepl-comm/make-unrepl-comm)]
    (f unrepl-comm))) ; TODO: close

(defn run-kernel [config]
  (let [hb-addr      (address config :hb_port)
        shell-addr   (address config :shell_port)
        iopub-addr   (address config :iopub_port)
        control-addr (address config :control_port)
        stdin-addr   (address config :stdin_port)
        key          (:key config)]
    (let [alive  (atom true)
          context (zmq/context 1)
          shell-socket   (doto (zmq/socket context :router)
                           (zmq/bind shell-addr))
          iopub-socket   (doto (zmq/socket context :pub)
                           (zmq/bind iopub-addr))
          control-socket (doto (zmq/socket context :router)
                           (zmq/bind control-addr))
          stdin-socket   (doto (zmq/socket context :router)
                           (zmq/bind stdin-addr))
          hb-socket      (doto (zmq/socket context :rep)
                           (zmq/bind hb-addr))
          sockets       {:shell-socket shell-socket, :iopub-socket iopub-socket, :stdin-socket stdin-socket, :control-socket control-socket}
          status-sleep 1000]
      (with-unrepl-comm
        (fn [nrepl-comm]
          (try
            (reset! (beckon/signal-atom "INT") #{(fn [] #_(pp/pprint (pnrepl/nrepl-interrupt nrepl-comm)))})
            (shell-loop     alive sockets nrepl-comm key)
            (control-loop   alive sockets nrepl-comm key)
            (heartbeat-loop alive hb-socket)
            ;; check every second if state
            ;; has changed to anything other than alive
            (while @alive (Thread/sleep status-sleep))
            (catch Exception e
              (exception-handler e))
            (finally (doseq [socket [shell-socket iopub-socket control-socket hb-socket]]
                       (zmq/set-linger socket 0)
                       (zmq/close socket))
                     (System/exit 0))))))))

(defn -main [& args]
  (log/set-level! :error)
  (run-kernel (prep-config args)))
