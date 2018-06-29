(ns clojupyter.core
  (:require [beckon]
    [cheshire.core :as json]
    [clojure.java.io :as io]
    [clojupyter.misc.unrepl-comm :as unrepl-comm]
    [clojupyter.misc.messages :refer :all]
    [cheshire.core :as json]
    [clojure.stacktrace :as st]
    [clojure.walk :as walk]
    [clojure.core.async :as a]
    [clojure.string :as str]
    [taoensso.timbre :as log]
    [zeromq.zmq :as zmq]
    [net.cgrand.packed-printer :as pp])
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
          repl (atom nil)
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
                    (if-some [[_ command args] (re-matches #"(?s)\s*/(\S+?)([\s,\[{(].*)?" code)]
                      (case command
                        "connect" (let [args (re-seq #"\S+" args)]
                                    (try
                                      (let [[_ host port inner] (re-matches #"(?:(?:(\S+):)?(\d+)|(-))" (first args))
                                            connector (if inner
                                                        #(let [{in-writer :writer in-reader :reader} (unrepl-comm/pipe)
                                                               {out-writer :writer out-reader :reader} (unrepl-comm/pipe)]
                                                           (a/thread
                                                             (binding [*out* out-writer *in* (clojure.lang.LineNumberingPushbackReader. in-reader)]
                                                               (clojure.main/repl)))
                                                           {:in in-writer
                                                            :out out-reader})
                                                        #(let [socket (java.net.Socket. ^String host (Integer/parseInt port))]
                                                           {:in (-> socket .getOutputStream io/writer)
                                                            :out (-> socket .getInputStream io/reader)}))]
                                        (reset! repl (unrepl-comm/unrepl-connect connector))
                                        (broadcast "stream" {:name "stdout" :text "Successfully connected!"}))
                                     (catch Exception e
                                       (broadcast "stream" {:name "stderr" :text "Failed connection."})))
                                    (reply {:status "ok"
                                            :execution_count execution-count
                                            :user_expressions {}}))
                        #_#_"cp" (try
                                   (let [arg (edn/read-string args)
                                         paths
                                         (cond
                                           (map? arg)
                                           (let [deps (if (every? symbol? (keys arg))
                                                        {:deps arg}
                                                        arg)
                                                 libs (deps/resolve-deps deps {})]
                                             (into [] (mapcat :paths) (vals libs)))
                                           (string? arg) [arg]
                                           :else (throw (IllegalArgumentException. (str "Unsupported /cp argument: " arg))))]
                                     (doseq [path paths]
                                       (.addURL class-loader (-> path java.io.File. .toURI .toURL)))
                                     (stdout (str paths " added to the classpath!"))
                                     {:result "nil"})
                      (catch Exception e
                        (stderr "Something unexpected happened.")
                        {:result "nil"}))
                        (if-some [elided nil #_(elisions/lookup command)]
                          #_(-> elided :form :get do-eval) ; todo check that reachable or that's an elision
                         (do
                         (broadcast "stream" {:name "stderr" :text (str "Unknown command: /" command ".")})
                           (reply {:status "ok"
                                   :execution_count execution-count
                                   :user_expressions {}}))))
                      (if-some [{:keys [in edn-out]} @repl]
                       (do
                         (doto in (.write (prn-str `(eval (read-string ~code)))) .flush)
                         (loop [done false]
                           (prn 'WAITING)
                           (if-some [[tag payload id :as msg] (a/<!! edn-out)]
                             (do (prn 'GOT msg)
                               (case tag
                                :prompt (when-not done (recur done))
                                :eval (do
                                        (broadcast "execute_result"
                                          {:execution_count execution-count
                                           :data {:text/plain (with-out-str (pp/pprint payload :as :unrepl/edn :strict 20 :width 72))
                                                  #_#_:text/html (html/html payload)}
                                           :metadata {}})
                                        (reply {:status "ok"
                                                :execution_count execution-count
                                                :user_expressions {}})
                                        (recur true))
                                :exception (let [error
                                                 {:status "error"
                                                  :ename "Oops"
                                                  :evalue ""
                                                  :execution_count execution-count
                                                  :traceback (let [{:keys [ex phase]} payload]
                                                               [(str "Exception while " (case phase :read "reading the expression" :eval "evaluating the expression"
                                                                                          :print "printing the result" "doing something unexpected") ".")
                                                                (with-out-str (pp/pprint ex :as :unrepl/edn :strict 20 :width 72))])}]
                                             (broadcast "error" (dissoc error :status :execution_count))
                                             (reply error)
                                             (recur true))
                                :out
                                (do
                                  (broadcast "stream" {:name "stdout" :text payload})
                                  (recur done))
                                :err
                                (do
                                  (broadcast "stream" {:name "stderr" :text payload})
                                  (recur done))
                                (recur done)))
                             (throw (ex-info "edn output from unrepl unexpectedly closed; the connection to the repl has probably been interrupted.")))))
                       (let [error
                             {:status "error"
                              :ename "Oops"
                              :evalue ""
                              :execution_count execution-count
                              :traceback ["Not connected, use /connect host:port or /connect - (for local)" ""]}]
                         (broadcast "error" (dissoc error :status :execution_count))
                         (reply error)))))
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
