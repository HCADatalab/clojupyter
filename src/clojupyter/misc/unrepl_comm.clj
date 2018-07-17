(ns clojupyter.misc.unrepl-comm
  (:require [clojupyter.misc.messages :refer :all]
            [clojure.core.async :as a]
            [clojure.walk :as w]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [cheshire.core :as json]
            [clojupyter.print.text]
            [clojupyter.print.html-pre :as html]
            [net.cgrand.packed-printer :as pp]
            [clojupyter.unrepl.elisions :as elisions]
            [clojure.tools.deps.alpha :as deps]))

(defn pipe []
  (let [pipe (java.nio.channels.Pipe/open)]
    {:reader (-> pipe .source java.nio.channels.Channels/newInputStream (java.io.InputStreamReader. "UTF-8"))
     :writer (-> pipe .sink java.nio.channels.Channels/newOutputStream (java.io.OutputStreamWriter. "UTF-8"))}))

(defn- ^java.io.PushbackReader hello-sync
  "Takes the output of a freshly started unrepl and waits for the unrepl welcome message.
   Returns a reader suitable for edn/read."
  [out]
  (let [^java.io.BufferedReader out (cond-> out (not (instance? java.io.BufferedReader out)) java.io.BufferedReader.)]
    (loop [] ; sync on hello
      (when-some [line (.readLine out)]
        (if-some [[_ hello] (re-matches #".*?(\[:unrepl/hello.*)" line)]
          (doto (java.io.PushbackReader. out (inc (count hello)))
            (.unread (int \newline))
            (.unread (.toCharArray hello)))
          (recur))))))

(defn edn-reader-ch
  "Takes a reader suitable for edn/read (as returned by hello-sync) and returns a channel to which read
   edn messages are written.
   Closing the channel closes the reader."
  [out]
  (let [out (hello-sync out)
        ch (a/chan)
        eof (Object.)]
    (a/thread
      (try
        (loop []
          (let [x (edn/read {:eof eof :default tagged-literal} out)]
            (cond
              (identical? eof x) (a/close! ch)
              (a/>!! ch x) (recur)
              :else (a/close! ch))))
        (finally
          (.close out))))
    ch))

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
              resp (when-some [url (when path
                                     (or (io/resource (str "blob-libs/" path))
                                       (.getResource cl path)))]
                     (let [bout (java.io.ByteArrayOutputStream.)]
                       (with-open [cin (.openStream url)
                                   b64out (.wrap (java.util.Base64/getEncoder) bout)]
                         (io/copy cin b64out))
                       (String. (.toByteArray bout) "ASCII")))]
          (binding [*out* in] (prn resp)))
        (recur)))))

(defn emit-action [template args-map]
  (w/postwalk
    (fn [x]
      #(cond-> %
         (and (tagged-literal? %) (= (:tag %) :unrepl/param)) (-> :form args-map)))
    template))

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
               unrepl-output (if-some [[tag payload id] val]
                               (case tag
                                 :unrepl/hello
                                 (do
                                   (when on-hello (on-hello payload))
                                   (recur offset all-caught-up id msgs-out))
                                 ; misaligned forms are not tracked because all input is framed
                                 #_#_:read
                                   (recur offset (transduce (take-while (fn [[end-offset]] (< end-offset (:offset payload)))) 
                                                   (completing (fn [evals _] (pop evals))) evals evals))
                                 :prompt
                                 (recur offset (<= offset (:offset payload)) id msgs-out)
                                 #_#_:started-eval nil
                                 
                                 (:eval :exception) (do (some-> msgs-out (doto (a/>! val) a/close!)) (recur offset all-caught-up nil nil))
                          
                                 ; else
                                 ; todo filter by id
                                 (do (some-> msgs-out (a/>! val)) (recur offset all-caught-up eval-id msgs-out)))
                               (do (some-> msgs-out (a/>! [:err "Connection to repl has been lost!" nil]) a/close!) (recur offset all-caught-up nil nil))))))
    to-eval))

(defn unrepl-connect
  [connector #_class-loader]
  (let [{:keys [^java.io.Writer in ^java.io.Reader out]} (connector)]
    (with-open [blob (io/reader (io/resource "unrepl-blob.clj") :encoding "UTF-8")]
      (io/copy blob in))
    (.flush in)
    {:in in
     :edn-out (edn-reader-ch out)}))

(defn aux-connect
  [connector form]
  (let [{:keys [^java.io.Writer in ^java.io.Reader out]} (connector)]
    (doto in
      (.write (prn-str form))
      .flush)
    {:in in
     :edn-out (edn-reader-ch out)}))

(defn unrepl-client
  "Creates a client from a connector.
   A connector is a function of no-arg that returns a fresh pair of streams (as a map):
   a Writer (in) and a Reader (out) -- it's inverted
   because we are considering input and output relativeley to the repl, not
   to the client."
  [connector class-loader]
  (let [{:keys [^java.io.Writer in ^java.io.Reader out]} (connector)
        aux-ch (atom nil)
        actions (atom {})]
    (with-open [blob (io/reader (io/resource "unrepl-blob.clj") :encoding "UTF-8")]
      (io/copy blob in))
    (.flush in)
    (let [user-ch (client-loop in out
                    :on-hello
                    (fn [payload]
                      (let [{:keys [start-aux :unrepl.jvm/start-side-loader complete]} (swap! actions into (:actions payload))]
                        (when start-aux
                          (let [{:keys [^java.io.Writer in ^java.io.Reader out]} (connector)
                                _ (binding [*out* in] (prn start-aux))]
                            (reset! aux-ch (client-loop in out))))
                        (when start-side-loader
                          (let [{:keys [^java.io.Writer in ^java.io.Reader out]} (connector)
                                _ (binding [*out* in] (prn start-side-loader))]
                            (sideloader-loop in out class-loader))))))
          all-ch (a/chan)]
      (a/go-loop []
        (when-some [[tag payload back-ch] (a/<! all-ch)]
          (case tag
            :eval (a/>! user-ch [payload back-ch])
            :complete
            (let [{:keys [complete]} @actions]
              #_TODO)
            (a/>! @aux-ch [payload back-ch]))
          (recur)))
      all-ch)
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
  #_(let [unrepl-ch (atom nil)
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
                     (a/>!! eval-ch [:eval code msgs])
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
           (if-some [[_ command args] (re-matches #"(?s)\s*/(\S+?)([\s,\[{(].*)?" code)]
             (case command
               "connect" (let [args (re-seq #"\S+" args)]
                           (try
                             (let [[_ host port inner] (re-matches #"(?:(?:(\S+):)?(\d+)|(-))" (first args))
                                   connector (if inner
                                               #(let [{in-writer :writer in-reader :reader} (pipe)
                                                      {out-writer :writer out-reader :reader} (pipe)]
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
               (if-some [elided (elisions/lookup command)]
                 (-> elided :form :get do-eval) ; todo check that reachable or that's an elision
                 (do
                   (stderr (str "Unknown command: /" command "."))
                   {:result "nil"})))
             (do-eval code))))
       (nrepl-complete [self [left right]]
         (let [eval-ch @unrepl-ch ; TODO test
               msgs (a/chan)]
           (a/>!! eval-ch [:complete [left right] msgs])
           (a/<!! (a/go-loop []
                    (if-some [[tag payload] (a/<!! msgs)]
                      (case tag
                        :eval payload
                        :exception []
                        (recur))
                      []))))
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
