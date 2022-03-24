(ns de.npcomplete.coprolite.query
  (:require [clojure.string :as str]
            [de.npcomplete.coprolite.core :as core]
            [de.npcomplete.coprolite.util :as u]))

;; TODO (for later): consider moving away from macros for queries, so that they can be built dynamically at runtime

;; NOTE: make sure to pass symbols
(defn variable?
  "A predicate that accepts a string and checks whether it describes a datalog variable (either starts with ? or it is _)"
  ([x] (variable? x true))
  ([x accept-underscore?]                                   ; intentionally implemented as function and not a macro so we'd be able to use it as a HOF
   (and (symbol? x)
        (or (and accept-underscore? (= x '_))
            (str/starts-with? (name x) "?")))))

(defmacro clause-term-expr
  [clause-term]
  (cond
    (variable? clause-term)                                 ; variable
    `any?
    (not (coll? clause-term))                               ; constant
    `#(= % ~clause-term)
    ;; TODO: try to unify building these 3 cases of operator functions
    (= 2 (count clause-term))                               ; unary operator
    `#(~(first clause-term) %)
    (variable? (second clause-term))                        ; binary operator, 1st operand is variable
    `#(~(first clause-term) % ~(last clause-term))
    (variable? (last clause-term))                          ; binary operator, 2nd operand is variable
    `#(~(first clause-term) ~(second clause-term) %)
    :error
    (throw (ex-info "Term not matched by any condition" {:clause-term clause-term}))))

(defmacro clause-term-variable
  "Returns the (first) variable of the given term"
  [clause-term]
  (->> (u/collify clause-term)
       (filter #(variable? % false))
       first))

(defmacro pred-clause
  "Build a predicate clause from a 3 element query EAV clause."
  [clause]
  (with-meta
    `[~@(map (fn [x] `(clause-term-expr ~x)) clause)]
    {:db/variable `[~@(map (fn [x] `(clause-term-variable ~x)) clause)]}))

(defmacro q-clauses-to-pred-clauses
  [clauses]
  (loop [[clause & remaining-clauses] clauses
         predicate-vectors []]
    (if-not clause
      predicate-vectors
      (recur remaining-clauses
        (conj predicate-vectors `(pred-clause ~clause))))))


;; NOTE: the tutorial converts to a set of strings
(defmacro symbol-col-to-set
  [coll]
  (set coll))


;; query planning

(defn single-index-query-plan
  [query indx db]
  (let [q-res (query-index (core/index-at db indx) query)]
    (bind-variables-to-query q-res (core/index-at db indx))))

(defn ^:private collapse-variable-vectors
  [accV v]
  (mapv #(when (= %1 %2) %1) accV v))

(defn index-of-joining-variable
  ^long [query-clauses]
  (->> query-clauses
       (mapv #(:db/variable (meta %)))
       (reduce collapse-variable-vectors)
       (keep-indexed #(when (variable? %2 false) %1))
       first))

(defn build-query-plan
  [query]
  (let [join-index (index-of-joining-variable query)
        db-index   (case join-index 0 :AVET 1 :VEAT 2 :EAVT)]
    (partial single-index-query-plan query db-index)))
