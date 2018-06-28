(ns clojupyter.core
  (:require [beckon]
    [cheshire.core :as json]
    [clojupyter.misc.unrepl-comm :as unrepl-comm]
    [clojupyter.misc.messages :refer :all]
    [cheshire.core :as json]
    [clojure.pprint :as pp]
    [clojure.stacktrace :as st]
    [clojure.walk :as walk]
    [clojure.core.async :as a]
    [clojure.string :as str]
    [taoensso.timbre :as log]
    [zeromq.zmq :as zmq])
  (:import [java.net ServerSocket])
  (:gen-class :main true))

(defn prep-config [args]
  (-> args
      first
      slurp
      (json/parse-string keyword)))

(defn exception-handler [e]
  (log/error (with-out-str (st/print-stack-trace e 20))))

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

(defn process-event [alive sockets socket key handler]
  (let [message        (parts-to-message (zmq/receive-all (sockets socket)))
        parsed-message (parse-message message)
        parent-header  (:header parsed-message)
        session-id     (:session parent-header)]
    (send-message (:iopub-socket sockets) "status"
      {:execution_state "busy"} parent-header session-id {} key)
    (handler parsed-message)
    (send-message (:iopub-socket sockets) "status"
      {:execution_state "idle"} parent-header session-id {} key)))

(defn zmq-ch [socket]
  (let [ch (a/chan)]
    (a/thread
      (try
        (while (->> socket zmq/receive-all parts-to-message parse-message (a/>!! ch)))
        (catch Exception e
          (exception-handler e))
        (finally
          (zmq/set-linger socket 0)
          (zmq/close socket))))
    ch))

(defn heartbeat-loop [alive hb-socket]
  (a/thread
    (try
      (while @alive
        (zmq/send hb-socket (zmq/receive hb-socket)))
      (catch Exception e
        (exception-handler e))
      (finally
        (zmq/set-linger hb-socket 0)
        (zmq/close hb-socket)))))

(defn address [config service]
  (str (:transport config) "://" (:ip config) ":" (service config)))

(defn is-complete?
  "Returns whether or not what the user has typed is complete (ready for execution).
   Not yet implemented. May be that it is just used by jupyter-console."
  [code]
  (try
    (or (re-matches #"\s*/(\S+)\s*(.*)" code) ; /command
      (read-string code))
    true
    (catch Exception _
      false)))

(defn run-kernel [config]
  (let [hb-addr      (address config :hb_port)
        shell-addr   (address config :shell_port)
        iopub-addr   (address config :iopub_port)
        control-addr (address config :control_port)
        stdin-addr   (address config :stdin_port)
        key          (:key config)]
    (let [alive  (atom true)
          context (zmq/context 1)
          shell-socket (doto (zmq/socket context :router) (zmq/bind shell-addr))
          shell (zmq-ch shell-socket)
          control-socket (doto (zmq/socket context :router) (zmq/bind control-addr))
          control (zmq-ch control-socket)
          iopub-socket (doto (zmq/socket context :pub) (zmq/bind iopub-addr))
          stdin-socket (doto (zmq/socket context :router) (zmq/bind stdin-addr))
          stdin (zmq-ch stdin-socket)
          status-sleep 1000
          unrepl-comm (unrepl-comm/make-unrepl-comm)
          execution-counter (atom 1)
          shell-handler
          (fn [{{msg-type :msg_type session :session :as header} :header idents :idents :as request} socket]
            (letfn [(broadcast [msg-type content]
                      (send-message iopub-socket msg-type
                        content header session {} key))
                    (reply
                      ([content] (reply content {}))
                      ([content metadata]
                        (let [[_ msg-prefix] (re-matches #"(.*)_request" msg-type)]
                          (send-message socket (str msg-prefix "_reply")
                            content header session metadata key idents))))]
              (broadcast "status" {:execution_state "busy"})
              (try
                (case msg-type
                  "execute_request"
                  (let [execution-count (swap! execution-counter inc)
                        code (get-in request [:content :code])
                        silent (str/ends-with? code ";")]
                    (broadcast "execute_input" {:execution_count execution-count :code code})
                    (let [nrepl-resp {:result (json/generate-string {:text/plain "Yo!"})}
                          #_(pnrepl/nrepl-eval nrepl-comm alive sockets
                              code parent-header
                              session-id key idents)
                          {:keys [result ename traceback]} nrepl-resp
                          error (if ename
                                  {:status "error"
                                   :ename ename
                                   :evalue ""
                                   :execution_count execution-count
                                   :traceback traceback})]
                      (reply (or error
                               {:status "ok"
                                :execution_count execution-count
                                :user_expressions {}}))
                      (cond
                        error (broadcast "error" (dissoc error :status :execution_count))
                        (not (or (= result "nil") silent))
                        (broadcast "execute_result"
                          {:execution_count execution-count
                           :data (json/parse-string result true)
                           :metadata {}}))))
                  "kernel_info_request"
                  (reply (kernel-info-content))
                  "shutdown_request"
                  (do
                    (reset! alive false)
                    #_(nrepl.server/stop-server server)
                    (reply {:status "ok" :restart false})
                    (Thread/sleep 100)) ; magic timeout! TODO fix
                
                 ; COMMs were not handled anyway
                 ; see http://jupyter-notebook.readthedocs.io/en/stable/comms.html
                 ; and http://jupyter-client.readthedocs.io/en/stable/messaging.html
                 #_#_"comm_info_request"
                   (send-message socket "comm_info_reply"
                     {:comms {:comm_id {:target_name ""}}} header session {} key) ; no idents?
                 #_#_"comm_msg"
                   (send-message socket "comm_msg_reply"
                     {} header session {} key)
                 #_#_"comm_open"           (comm-open-reply   sockets
                                             socket message key)
              
                 "is_complete_request"
                 (reply {:status (if (-> request :content :code is-complete?) "complete" "incomplete")})
                 "complete_request"
                 (let [{:keys [code cursor_pos]} (:content request)
                       left (subs code 0 cursor_pos)
                       right (subs code cursor_pos)]
                   (reply
                     {#_#_:matches (pnrepl/nrepl-complete nrepl-comm [left right])
                      :cursor_start cursor_pos #_(- cursor_pos (count sym)) ; TODO fix
                      :cursor_end cursor_pos
                      :status "ok"}))
              
                 #_#_"interrupt_request" TODO
                 
                 (do
                   (log/error "Message type" msg-type "not handled yet. Exiting.")
                   (log/error "Message dump:" request)
                   (System/exit -1)))
                (finally
                  (broadcast "status" {:execution_state "idle"})))))]
      
      (heartbeat-loop alive (doto (zmq/socket context :rep) (zmq/bind hb-addr)))
      
      (a/go-loop [state {}]
        (a/alt!
          shell ([request] (shell-handler request shell-socket))
          control ([request] (shell-handler request control-socket))
          #_#_iopub 
          ([{{msg-type :msg_type} :header :as request}]
            (case msg-type
              #_#_"input_reply" TODO
                            
              (do
                (log/error "Message type" msg-type "not handled yet. Exiting.")
                (log/error "Message dump:" message)
                (System/exit -1)))))
        (recur state))
      
      #_(try
         (reset! (beckon/signal-atom "INT") #{(fn [] #_(pp/pprint (pnrepl/nrepl-interrupt nrepl-comm)))})
         (control-loop   alive sockets nrepl-comm key)
         ;; check every second if state
         ;; has changed to anything other than alive
         (while @alive (Thread/sleep status-sleep))
         (catch Exception e
           (exception-handler e))
         (finally (doseq [socket [shell-socket iopub-socket control-socket hb-socket]]
                    (zmq/set-linger socket 0)
                    (zmq/close socket))
                  (System/exit 0))))))

(defn -main [& args]
  (log/set-level! :error)
  (run-kernel (prep-config args)))
