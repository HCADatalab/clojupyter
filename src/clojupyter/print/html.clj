(ns clojupyter.print.html
  (:require [clojupyter.unrepl.elisions :as elisions]
    [hiccup.core :as h]))

(defn html [x]
  (letfn [#_#_(coll-spans
              ([x] (coll-spans x [te/space] spans))
              ([x sp spans]
                (sequence (comp (map spans) (interpose sp) cat) x)))
          (kv-spans [[k v]]
            (if (elisions/elision? k)
              (spans v)
              (-> [kv-open] (into (spans k)) (conj te/space) (into (spans v)) (conj te/kv-close))))
          (tree [x]
            (cond
              (keyword? x) [:span.keyword (str x)]
              (tagged-literal? x)
              (case (:tag x)
                unrepl/... (if-some [id (elisions/intern x)]
                             [:span.elision {:data-elision-id id}]
                             [:span.unreachable])
                clojure/var [:span.var (str (:form x))]
                unrepl/meta (let [[m v] (:form x)]
                              [:span.meta (tree m) (tree v)])
                unrepl.java/class [:span.class (str (:form x))] ; to distinguish from symbols
                unrepl/string (let [[s e] (:form x)
                                    s (pr-str s)]
                                [:span.string
                                 (subs s 1 (dec (count s)))
                                 (tree e)])
                unrepl/ratio (let [[n d] (:form x)] (str n "/" d))
                unrepl/pattern (let [[n d] (:form x)]
                                 (pr-str (re-pattern (:form x))))
                #_#_unrepl/lazy-error
                (concat [kv-open (ansi (str "/lazy-error")
                                   (str "\33[31m/lazy-error\33[m"))
                         te/space]
                  (spans (-> x :form :form :cause))
                  [te/space
                   (let [cmd (str "/" (elisions/intern (:form x)))]
                     (ansi cmd (str "\33[31m\33[4m" cmd "\33[m")))
                   te/kv-close])
                #_#_error (concat
                             [kv-open (ansi (str "#" (pr-str (:tag x)))
                                        (str "\33[31m#" (pr-str (:tag x)) "\33[m"))
                              te/space]
                             (spans (:form x)) [te/kv-close])
                #_(concat [kv-open (str "#" (pr-str (:tag x))) te/space] (spans (:form x)) [te/kv-close]))
              (string? x) (let [s (pr-str x)]
                            [:span.string (subs s 1 (dec (count s)))])
              (vector? x) (into [:span.vector] (map tree) x)
              (set? x) 
              (-> [:span.set] 
                (into (comp (remove elisions/elision?) (map tree)) x)
                (into (comp (filter elisions/elision?) (map tree)) x))
              (seq? x) (into [:span.seq] (map tree) x)
              #_#_(map? x) (if-some [kv (find x elisions/unreachable)]
                             (concat [(delims "{")] (coll-spans (concat (dissoc x elisions/unreachable) [kv]) [comma te/space] kv-spans) [(delims "}")])
                             (concat [(delims "{")] (coll-spans x [comma te/space] kv-spans) [(delims "}")]))
              :else (pr-str x)))]
    (h/html [:div.clj [:style ".clj .keyword {color: navy;} .clj .vector::before {content: \"[\"} .clj .vector::after {content: \"]\"}"] (tree x)])))