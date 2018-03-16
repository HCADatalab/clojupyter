(ns clojupyter.misc.display
  (:require [clojupyter.protocol.mime-convertible :as mc]
            [hiccup.core :as hiccup]))

#_(defn display [obj]
   ; this function was meant as a kind of rich-print, commenting out for now
   (swap! (:display-queue @states/current-global-states) conj (mc/to-mime obj))
   nil)


;; Html

(defrecord HtmlString [html]

  mc/PMimeConvertible
  (to-mime [_]
    (mc/stream-to-string
     {:text/html html})))

(defn html [html-str]
  (HtmlString. html-str))

(defn ^:deprecated make-html
  [html-str]
  (html html-str))

(defrecord HiccupHTML [html-data]

  mc/PMimeConvertible
  (to-mime [_]
    (mc/stream-to-string
     {:text/html (hiccup/html html-data)})))

(defn hiccup-html [html-data]
  (HiccupHTML. html-data))

;; Latex

(defrecord Latex [latex]

  mc/PMimeConvertible
  (to-mime [_]
    (mc/stream-to-string
     {:text/latex latex})))

(defn latex [latex-str]
  (Latex. latex-str))

(defn ^:deprecated make-latex
  [latex-str]
  (latex latex-str))


;; Markdown

(defrecord Markdown [markdown]

  mc/PMimeConvertible
  (to-mime [_]
    (mc/stream-to-string
     {:text/markdown markdown})))

(defn markdown [markdown-str]
  (Markdown. markdown-str))

(defn ^:deprecated make-markdown
  [markdown-str]
  (markdown markdown-str))
