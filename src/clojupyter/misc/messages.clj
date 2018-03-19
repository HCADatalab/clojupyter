(ns clojupyter.misc.messages
  (require
   [cheshire.core :as cheshire]
   [clj-time.core :as time]
   [clj-time.format :as time-format]
   [clojupyter.misc.complete :as complete]
   [clojupyter.protocol.nrepl-comm :as pnrepl]
   [clojure.pprint :as pp]
   [clojure.string :as str]
   [clojure.tools.nrepl :as nrepl]
   [clojure.tools.nrepl.misc :as nrepl.misc]
   [clojure.tools.nrepl.server :as nrepl.server]
   [pandect.algo.sha256 :refer [sha256-hmac]]
   [taoensso.timbre :as log]
   [zeromq.zmq :as zmq]))

(def protocol-version "5.0")

(defn uuid [] (str (java.util.UUID/randomUUID)))

(defn now []
  "Returns current ISO 8601 compliant date."
  (let [current-date-time (time/to-time-zone (time/now) (time/default-time-zone))]
    (time-format/unparse
     (time-format/with-zone (time-format/formatters :date-time-no-ms)
       (.getZone current-date-time))
     current-date-time)))

(defn message-header [message msgtype]
  (cheshire/generate-string
   {:msg_id (uuid)
    :date (now)
    :username (get-in message [:header :username])
    :session (get-in message [:header :session])
    :msg_type msgtype
    :version protocol-version}))

(defn new-header [msg_type session-id]
  {:date (now)
   :version protocol-version
   :msg_id (uuid)
   :username "kernel"
   :session session-id
   :msg_type msg_type})

(defn send-message
  ([socket msg-type content parent-header session-id metadata signer]
    (send-message socket msg-type content parent-header session-id metadata signer [(.getBytes ^String msg-type)]))
  ([socket msg-type content parent-header session-id metadata signer idents]
    (let [header        (cheshire/generate-string (new-header msg-type session-id))
          parent-header (cheshire/generate-string parent-header)
          metadata      (cheshire/generate-string metadata)
          content       (cheshire/generate-string content)]
     (doseq [ident idents]
       (zmq/send socket ident zmq/send-more))
     (doseq [^String part ["<IDS|MSG>"
                           (signer header parent-header metadata content)
                           header
                           parent-header
                           metadata]]
       (zmq/send socket (.getBytes part) zmq/send-more))
     (zmq/send socket (.getBytes ^String content)))))

(defn get-message-signer [key]
  "returns a function used to sign a message"
  (if (empty? key)
    (fn [header parent metadata content] "")
    (fn [header parent metadata content]
      (sha256-hmac (str header parent metadata content) key))))

(defn get-message-checker [signer]
  "returns a function to check an incoming message"
  (fn [{:keys [signature header parent-header metadata content]}]
    (let [our-signature (signer header parent-header metadata content)]
      (= our-signature signature))))

(defn parse-message [message]
  {:idents (:idents message)
   :delimiter (:delimiter message)
   :signature (:signature message)
   :header (cheshire/parse-string (:header message) keyword)
   :parent-header (cheshire/parse-string (:parent-header message) keyword)
   :content (cheshire/parse-string (:content message) keyword)})

;; Message contents

(defn status-content [status]
  {:execution_state status})

(defn pyin-content [execution-count message]
  {:execution_count execution-count
   :code (get-in message [:content :code])})

(defn is-complete-reply-content
  "Returns whether or not what the user has typed is complete (ready for execution).
   Not yet implemented. May be that it is just used by jupyter-console."
  [message]
  (if (complete/complete? (:code (:content message)))
    {:status "complete"}
    {:status "incomplete"})
  )

(defn complete-reply-content
  [nrepl-comm
   message]
  (let [find-symbol (fn [code]
                      (loop
                          [pairs (map-indexed vector (reverse code))
                           matched ""
                           delimiter  nil]
                        (let [character (second (first pairs))
                              delimiter-map {\( :code
                                             \" :string
                                             \% :magic}
                              delimiter (get delimiter-map character)]
                          (if (and (not-empty pairs)
                                   (nil? delimiter))
                            (recur (rest pairs) (str character matched) delimiter)
                            [(last (str/split matched #" "))
                             delimiter]))))
        content (:content message)
        cursor_pos (:cursor_pos content)
        code (subs (:code content) 0 cursor_pos)
        [sym sym_type] (find-symbol code)]
    {:matches (pnrepl/nrepl-complete nrepl-comm sym)
     :cursor_start (- cursor_pos (count sym))
     :cursor_end cursor_pos
     :status "ok"}))

(defn kernel-info-content []
  {:status "ok"
   :protocol_version protocol-version
   :implementation "iclojure"
   :language_info {:name "clojure"
                   :version (clojure-version)
                   :mimetype "text/x-clojure"
                   :file_extension ".clj"}
   :banner "IClojure-0.1.0"
   :help_links []})

(defn comm-open-reply-content [message]
  {:comm_id (get-in message [:content :comm_id])
   :data {}})

;; Request and reply messages

(defn input-request
  [sockets parent-header session-id signer idents]
  (let [metadata {}
        content  {:prompt ">> "
                  :password false}]
    (send-message (:stdin-socket sockets)
                         "input_request"
                         content parent-header session-id metadata signer idents)))

(defn comm-open-reply
  [sockets
   socket message signer]
  "Just close a comm immediately since we don't handle it yet"
  (let [parent-header (:header message)
        session-id (get-in message [:header :session])
        idents (:idents message)
        metadata {}
        content  (comm-open-reply-content message)]
    (send-message (sockets socket)
                         "comm_close"
                         content parent-header session-id metadata signer idents)))

(defn kernel-info-reply
  [sockets
   socket message signer]
  (let [parent-header (:header message)
        session-id (get-in message [:header :session])
        idents (:idents message)
        metadata {}
        content  (kernel-info-content)]
    (send-message (sockets socket)
                         "kernel_info_reply"
                         content parent-header session-id metadata signer idents)))

(defn shutdown-reply
  [alive sockets nrepl-comm socket message signer]
  (let [parent-header (:header message)
        metadata {}
        restart (get-in message message [:content :restart])
        content {:restart restart :status "ok"}
        session-id (get-in message [:header :session])
        idents (:idents message)
        #_#_server @(:nrepl-server nrepl-comm)]
    (reset! alive false)
    #_(nrepl.server/stop-server server)
    (send-message (sockets socket)
                         "shutdown_reply"
                         content parent-header session-id metadata signer idents)
    (Thread/sleep 100)))

(defn comm-info-reply
  [sockets
   socket message signer]
  (let [parent-header (:header message)
        metadata {}
        content  {:comms
                  {:comm_id {:target_name ""}}}
        session-id (get-in message [:header :session])]
    (send-message (sockets socket) "comm_info_reply"
                  content parent-header session-id metadata signer)))

(defn comm-msg-reply
  [sockets
   socket message socket signer]
  (let [parent-header (:header message)
        metadata {}
        content  {}
        session-id (get-in message [:header :session])]
    (send-message (sockets socket) "comm_msg_reply"
                  content parent-header session-id metadata signer)))

(defn is-complete-reply
  [sockets
   socket message signer]
  (let [parent-header (:header message)
        metadata {}
        content  (is-complete-reply-content message)
        session-id (get-in message [:header :session])
        idents (:idents message)]
    (send-message (sockets socket)
                         "is_complete_reply"
                         content parent-header session-id metadata signer idents)))

(defn complete-reply
  [sockets nrepl-comm
   socket message signer]
  (let [parent-header (:header message)
        metadata {}
        content  (complete-reply-content nrepl-comm message)
        session-id (get-in message [:header :session])
        idents (:idents message)]
    (send-message (sockets socket)
                         "complete_reply"
                         content parent-header session-id metadata signer idents)))

(defn history-reply
  [sockets
   socket message signer]
  "returns REPL history, not implemented for now and returns a dummy message"
  {:history []})

;; Handlers

(defn execute-request-handler
  [alive sockets nrepl-comm socket]
  (let [execution-count (atom 1N)]
    (fn [message signer]
      (let [session-id (get-in message [:header :session])
            idents (:idents message)
            parent-header (:header message)
            code (get-in message [:content :code])
            silent (str/ends-with? code ";")]
        (send-message (:iopub-socket sockets) "execute_input"
                      (pyin-content @execution-count message)
                      parent-header session-id {} signer)
        (let [nrepl-resp (pnrepl/nrepl-eval nrepl-comm alive sockets
                                            code parent-header
                                            session-id signer idents)
              {:keys [result ename traceback]} nrepl-resp
              error (if ename
                      {:status "error"
                       :ename ename
                       :evalue ""
                       :execution_count @execution-count
                       :traceback traceback})]
          (send-message (:shell-socket sockets) "execute_reply"
                               (if error
                                 error
                                 {:status "ok"
                                  :execution_count @execution-count
                                  :user_expressions {}})
                               parent-header
                               session-id
                               {}
                               signer idents)
          (if error
            (send-message (:iopub-socket sockets) "error"
                          error parent-header session-id {} signer)
            (when-not (or (= result "nil") silent)
              (send-message (:iopub-socket sockets) "execute_result"
                            {:execution_count @execution-count
                             :data (cheshire/parse-string result true)
                             :metadata {}}
                            parent-header session-id {} signer)))
          (swap! execution-count inc))))))
