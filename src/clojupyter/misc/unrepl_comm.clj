(ns clojupyter.misc.unrepl-comm
  (:require [cheshire.core :as cheshire]
            [clojupyter.protocol.nrepl-comm :as pnrepl]
            [clojupyter.misc.messages :refer :all]
            [clojure.tools.nrepl :as nrepl]
            [clojure.tools.nrepl.misc :as nrepl.misc]
            [clojure.core.async :as a]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [cheshire.core :as json]
            [clojupyter.print.text]
            [clojupyter.print.html-pre :as html]
            [net.cgrand.packed-printer :as pp]
            [clojupyter.unrepl.elisions :as elisions]))

(defn- hello-sync [out]
  (let [^java.io.BufferedReader out (cond-> out (not (instance? java.io.BufferedReader out)) java.io.BufferedReader.)]
    (loop [] ; sync on hello
      (when-some [line (.readLine out)]
        (if-some [[_ hello] (re-matches #".*?(\[:unrepl/hello.*)" line)]
          (doto (java.io.PushbackReader. out (inc (count hello)))
            (.unread (int \newline))
            (.unread (.toCharArray hello)))
          (recur))))))

(defn- sideloader-loop [^java.io.Writer in out ^ClassLoader cl]
  (let [out (java.io.PushbackReader. out)]
    (a/thread ; edn tuples reader
      (while (not= (edn/read out) [:unrepl.jvm.side-loader/hello]))
      (loop []
        (let [[type s] (edn/read out)
              path (case type
                     :resource s
                     :class (str (str/replace s "." "/") ".class")
                     nil)
              resp (when-some [url (some->> path (.getResource cl))]
                     (let [bout (java.io.ByteArrayOutputStream.)]
                       (with-open [cin (.openStream url)
                                   b64out (.wrap (java.util.Base64/getEncoder) bout)]
                         (io/copy cin b64out))
                       (String. (.toByteArray bout) "ASCII")))]
          (binding [*out* in] (prn resp)))
        (recur)))))

(defn- client-loop [^java.io.Writer in out & {:keys [on-hello]}]
  (let [to-eval (a/chan)
        unrepl-input (a/chan)
        unrepl-output (a/chan)
        [eof-tag :as eof] [(Object.)]
        out (hello-sync out)]
    (a/thread ; edn tuples reader
      (loop []
        (let [[tag :as msg] (edn/read {:eof eof :default tagged-literal} out)]
          (prn 'GOT msg) 
          (when-not (= eof-tag tag)
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
    (a/go-loop [offset 0 all-caught-up true eval-id nil msgs-out nil]
           (some-> msgs-out (cond-> all-caught-up a/close!))
           (let [[val ch] (a/alts! (cond-> [unrepl-output] all-caught-up (conj to-eval)))]
             (condp = ch
               to-eval (let [[code msgs] val
                             code (str code \newline)
                             offset (+ offset (count code))]
                         (a/>! unrepl-input code)
                         (recur offset false eval-id msgs))
               unrepl-output (let [[tag payload id] val]
                               (case tag
                                 :unrepl/hello
                                 (do
                                   (when on-hello (on-hello payload))
                                   (recur offset all-caught-up eval-id msgs-out))
                                 ; misaligned forms are not tracked because all input is framed
                                 #_#_:read
                                   (recur offset (transduce (take-while (fn [[end-offset]] (< end-offset (:offset payload)))) 
                                                   (completing (fn [evals _] (pop evals))) evals evals))
                                 :prompt
                                 (recur offset (<= offset (:offset payload)) id msgs-out)
                                 (:eval :exception) (do (some-> msgs-out (doto (a/>! val) a/close!)) (recur offset all-caught-up nil nil))
                          
                                 ; else
                                 ; todo filter by id
                                 (do (some-> msgs-out (a/>! val)) (recur offset all-caught-up eval-id msgs-out)))))))
    to-eval))

(defn unrepl-client
  "Creates a client from a connector.
   A connector is a function of no-arg that returns a fresh pair of streams (as a map):
   a Writer (in) and a Reader (out) -- it's inverted
   because we are considering input and output relativeley to the repl, not
   to the client."
  [connector class-loader]
  (let [{:keys [^java.io.Writer in ^java.io.Reader out]} (connector)]
    (with-open [blob (io/reader (io/resource "unrepl-blob.clj") :encoding "UTF-8")]
      (io/copy blob in))
    (.flush in)
    (client-loop in out
      :on-hello
      (fn [payload]
        (let [{:keys [start-aux :unrepl.jvm/start-side-loader]} (:actions payload)]
          #_(when start-aux
             (let [{:keys [^java.io.Writer in ^java.io.Reader out]} (connector)
                   _ (binding [*out* in] (prn start-aux))]
               (client-loop in out)))
          (when start-side-loader
            (let [{:keys [^java.io.Writer in ^java.io.Reader out]} (connector)
                  _ (binding [*out* in] (prn start-side-loader))]
              (sideloader-loop in out class-loader))))))
    #_{:eval to-eval
      :aux to-aux}))

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
  (let [unrepl-ch (atom nil)
        class-loader (clojure.lang.DynamicClassLoader. nil)]
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
      (nrepl-eval [self alive sockets code parent-header session-id signer ident]
        (let [get-input (fn [] (input-request sockets parent-header session-id signer ident))
              stdout     (fn [msg]
                           (send-message (:iopub-socket sockets) "stream"
                                         {:name "stdout" :text msg}
                                         parent-header session-id {} signer))
              stderr     (fn [msg]
                           (send-message (:iopub-socket sockets) "stream"
                                         {:name "stdout" :text msg}
                                         parent-header session-id {} signer))
              do-eval
              (fn [code]
                (if-some [eval-ch @unrepl-ch]
                  (let [msgs (a/chan)]
                    (a/>!! eval-ch [code msgs])
                    (loop [r nil]
                      (if-some [[tag payload] (a/<!! msgs)]
                        (recur
                          (case tag
                           :eval {:result (json/generate-string {:text/plain (with-out-str (pp/pprint payload :as :unrepl/edn :strict 20 :width 72))
                                                                 #_#_:text/html (html/html payload)})}
                           :exception {:ename "Oops"
                                       :traceback (let [{:keys [ex phase]} payload]
                                                    [(str "Exception while " (case phase :read "reading the expression" :eval "evaluating the expression"
                                                                               :print "printing the result" "doing something unexpected") ".")
                                                     (with-out-str (pp/pprint ex :as :unrepl/edn :strict 20 :width 72))])}
                           :out (do (stdout payload) r)
                           :err (do (stderr payload) r)
                           r))
                        r)))
                  (do 
                    (stderr "You need to connect first: /connect host:port")
                    {:result "nil" #_(json/generate-string {:text/plain "42"})})))]
          (if-some [[_ command args] (re-matches #"/(\S+)\s*(.*)" code)]
            (case command
              "connect" (let [args (re-seq #"\S+" args)]
                          (try
                            (let [[_ host port inner] (re-matches #"(?:(?:(\S+):)?(\d+)|(-))" (first args))
                                  connector (if inner
                                              #(let [in-writer (java.io.PipedWriter.)
                                                     in-reader (java.io.PipedReader. in-writer)
                                                     out-writer (java.io.PipedWriter.)
                                                     out-reader (java.io.PipedReader. out-writer)]
                                                 (a/thread
                                                   (binding [*out* out-writer *in* (clojure.lang.LineNumberingPushbackReader. in-reader)]
                                                     (clojure.main/repl)))
                                                 {:in in-writer
                                                  :out out-reader})
                                              #(let [socket (java.net.Socket. ^String host (Integer/parseInt port))]
                                                 {:in (-> socket .getOutputStream io/writer)
                                                  :out (-> socket .getInputStream io/reader)}))]
                              (reset! unrepl-ch (unrepl-client connector class-loader))
                              (stdout "Successfully connected!"))
                           (catch Exception e
                             (stderr "Failed connection.")))
                          {:result "nil"})
              "cp" (try
                     (let [arg (edn/read-string args)
                           f (java.io.File. arg)]
                       (.addURL class-loader (-> f .toURI .toURL))
                       (stdout (str (pr-str (.getCanonicalPath f)) " added to the classpath!")))
                     (catch Exception e
                       (stderr "Something unexpected happened. The argument ro /cp must be a string.")))
              (if-some [elided (elisions/lookup command)]
                (-> elided :form :get do-eval) ; todo check that reachable or that's an elision
                (do
                  (stderr (str "Unknown command: /" command "."))
                  {:result "nil"})))
            (do-eval code))))
      (nrepl-complete [self code]
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

; IPython.notebook.kernel.execute("(+ 1 1)", {iopub: {output: function() { console.log("reply", arguments) } }})
