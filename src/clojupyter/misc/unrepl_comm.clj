(ns clojupyter.misc.unrepl-comm
  (:require [cheshire.core :as cheshire]
            [clojupyter.protocol.nrepl-comm :as pnrepl]
            [clojupyter.misc.messages :refer :all]
            [clojupyter.protocol.zmq-comm :as pzmq]
            [clojure.tools.nrepl :as nrepl]
            [clojure.tools.nrepl.misc :as nrepl.misc]
            [clojure.core.async :as a]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [zeromq.zmq :as zmq]
            [cheshire.core :as json]
            [clojupyter.print.text]
            [clojupyter.print.html :as html]
            [net.cgrand.packed-printer :as pp]
            [clojupyter.unrepl.elisions :as elisions]))

(defn unrepl-client
  "Creates a client from a Writer (in) and a Reader (out) -- it's inverted
   because we are considering input and output relativeley to the repl, not
   to the client."
  [{:keys [^java.io.Writer in ^java.io.Reader out]}]
  (with-open [blob (io/reader (io/resource "unrepl-blob.clj") :encoding "UTF-8")] (io/copy blob in))
  (.flush in)
  (let [^java.io.BufferedReader out (cond-> out (not (instance? java.io.BufferedReader out)) java.io.BufferedReader.)
        out (loop [] ; sync on hello
              (when-some [line (.readLine out)]
                (if-some [[_ hello] (re-matches #".*?(\[:unrepl/hello.*)" line)]
                  (doto (java.io.PushbackReader. out (inc (count hello)))
                    (.unread (int \newline))
                    (.unread (.toCharArray hello)))
                  (recur))))
        unrepl-input (a/chan)
        unrepl-output (a/chan)
        to-eval (a/chan)
        [eof-tag :as eof] [(Object.)]]
    (a/thread ; edn tuples reader
      (loop []
        (let [[tag :as msg] (edn/read {:eof eof :default tagged-literal} out)]
          (prn 'GOT msg) 
          (when-not (= eof tag)
            (a/>!! unrepl-output msg)
            (if (= :bye tag)
              (a/close! unrepl-output msg)
              (recur)))))
      (a/close! unrepl-output))
    (a/thread ; text writer
      (loop []
        (when-some [^String s (a/<!! unrepl-input)]
          ; framed input because notebook style
          (doto in (.write (prn-str `(eval (read-string ~s)))) .flush)
          (recur))))
    (a/go-loop [offset 0 evals clojure.lang.PersistentQueue/EMPTY]
      (let [[val port] (a/alts! [to-eval unrepl-output])]
        (condp = port
          to-eval (let [[code promise] val
                        code (str code \newline)
                        offset (+ offset (count code))]
                    (a/>! unrepl-input code)
                    (recur offset (conj evals [offset promise])))
          unrepl-output (let [[tag payload id] val]
                          (case tag
                             ; misaligned forms are not tracked because all input is framed
                            #_#_:read
                            (recur offset (transduce (take-while (fn [[end-offset]] (< end-offset (:offset payload)))) 
                                            (completing (fn [evals _] (pop evals))) evals evals))
                            :eval
                            (let [[_ p] (peek evals)]
                              (deliver p {:value payload})
                              (recur offset (pop evals)))
                            :exception
                            (let [[_ p] (peek evals)]
                              (prn 'exception payload)
                              (deliver p {:exception payload})
                              (recur offset (pop evals)))
                            ; else
                            (recur offset evals))))))
    to-eval))

(defn stacktrace-string
  "Return a nicely formatted string."
  [msg]
  (when-let [st (:stacktrace msg)]
    (let [clean (->> st
                     (filter (fn [f] (not-any? #(= "dup" %) (:flags f))))
                     (filter (fn [f] (not-any? #(= "tooling" %) (:flags f))))
                     (filter (fn [f] (not-any? #(= "repl" %) (:flags f))))
                     (filter :file))
          max-file (apply max (map count (map :file clean)))
          max-name (apply max (map count (map :name clean)))]
      (map #(format (str "%" max-file "s: %5d %-" max-name "s")
                    (:file %) (:line %) (:name %))
           clean))))

(defn make-unrepl-comm []
  (let [sessions (atom {})]
    (reify
      pnrepl/PNreplComm
      (nrepl-trace [self]
        #_(-> (:nrepl-client self)
            (nrepl/message {:op :stacktrace
                            :session (:nrepl-session self)})
            nrepl/combine-responses
            doall))
      (nrepl-interrupt [self]
        #_(do
            (reset! interrupted true)
            (if (not @need-input)
              (-> (:nrepl-client self)
                (nrepl/message {:op :interrupt
                                :session (:nrepl-session self)})
                  nrepl/combine-responses
                  doall)
              ;; a special case here
              ;; seems like the interrupt :op
              ;; does not work when the repl server
              ;; is waiting for input
              ;; therefore do nothing and pretent (read-line)
              ;; return nil
              )
            ))
      (nrepl-eval [self states zmq-comm code parent-header session-id signer ident]
        (let [get-input (fn [] (input-request zmq-comm parent-header session-id signer ident))
              stdout     (fn [msg]
                           (send-message zmq-comm :iopub-socket "stream"
                                         {:name "stdout" :text msg}
                                         parent-header {} session-id signer))
              stderr     (fn [msg]
                           (send-message zmq-comm :iopub-socket "stream"
                                         {:name "stdout" :text msg}
                                         parent-header {} session-id signer))
              do-eval
              (fn [code]
                (if-some [eval-ch (@sessions session-id)]
                  (let [p (promise)]
                    (a/>!! eval-ch [code p])
                    (let [r @p]
                      (if (contains? r :value)
                        {:result (json/generate-string {:text/plain (with-out-str (pp/pprint (:value r) :as :unrepl/edn :strict 20 :width 72))
                                                        #_#_:text/html (html/html (:value r))})}
                        {:ename "Oops"
                         :traceback (let [{:keys [ex phase]} (:exception r)]
                                      [(str "Exception while " (case phase :read "reading the expression" :eval "evaluating the expression"
                                                                 :print "printing the result" "doing something unexpected") ".")
                                       (with-out-str (pp/pprint ex :as :unrepl/edn :strict 20 :width 72))])})))
                  (do 
                    (stderr "You need to connect first: /connect host:port")
                    {:result "nil" #_(json/generate-string {:text/plain "42"})})))]
          (if-some [[_ command args] (re-matches #"/(\S+)\s*(.*)" code)]
            (let [args (re-seq #"\S+" args)]
              (case command
                "connect" (do
                            (try
                              (let [[_ host port inner] (re-matches #"(?:(?:(\S+):)?(\d+)|(-))" (first args))
                                    streams (if inner
                                              (let [in-writer (java.io.PipedWriter.)
                                                    in-reader (java.io.PipedReader. in-writer)
                                                    out-writer (java.io.PipedWriter.)
                                                    out-reader (java.io.PipedReader. out-writer)]
                                                (a/thread
                                                  (binding [*out* out-writer *in* (clojure.lang.LineNumberingPushbackReader. in-reader)]
                                                    (clojure.main/repl)))
                                                {:in in-writer
                                                 :out out-reader})
                                              (let [socket (java.net.Socket. ^String host (Integer/parseInt port))]
                                                {:in (-> socket .getOutputStream io/writer)
                                                 :out (-> socket .getInputStream io/reader)}))]
                                (swap! sessions assoc session-id
                                  (unrepl-client streams))
                                (stdout "Successfully connected!"))
                             (catch Exception e
                               (stderr "Failed connection.")))
                            {:result "nil"})
                (if-some [elided (elisions/lookup command)]
                  (-> elided :form :get do-eval) ; todo check that reachable or that's an elision
                  (do
                    (stderr (str "Unknown command: /" command "."))
                    {:result "nil"}))))
            (do-eval code))))
      (nrepl-complete [self code]
        []
        #_(let [ns @current-ns
                result (-> (:nrepl-client self)
                           (nrepl/message {:op :complete
                                           :session (:nrepl-session self)
                                           :symbol code
                                           :ns ns})
                           nrepl/combine-responses)]
            (->> result
                 :completions
                 (map :candidate)
                 (into [])))))))
