(ns de.npcomplete.coprolite.query
  (:require [clojure.set :as set]
            [clojure.string :as str]
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

(defn clause-term-expr
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

(defn ^:private clause-term-variable-fn
  "Returns the (first) variable of the given term"
  [clause-term]
  (->> (u/collify clause-term)
       (filter #(variable? % false))
       first))

(defmacro pred-clause
  "Build a predicate clause from a 3 element query EAV clause."
  [clause]
  `(with-meta
     [~@(map clause-term-expr clause)]
     {:db/variable (quote [~@(map clause-term-variable-fn clause)])}))

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

(defn filter-index
  [index predicate-clauses]
  (for [[e a v :as pred-clause] predicate-clauses
        :let [[lvl1-pred lvl2-pred lvl3-pred] ((core/from-eav index) e a v)]
        ; keys and values of the first level
        [k1 l2map] index
        :when (try (lvl1-pred k1) (catch Exception _ false))
        ; keys and values of the second level
        [k2 l3-set] l2map
        :when (try (lvl2-pred k2) (catch Exception _ false))
        :let [res (into #{} (filter lvl3-pred) l3-set)]]
    (with-meta [k1 k2 res] (meta pred-clause))))

(defn items-that-answer-all-conditions
  [items-seq ^long num-of-conditions]
  (->> items-seq                                            ; take the sets of items
       (reduce into [])                                     ; reduce all the sets into one vector
       (frequencies)                                        ; count for each item in how many sets it was in
       (into #{}
         (keep (fn [[item ^long n]]
                 (when (<= num-of-conditions n)             ; items that answered all conditions
                   item))))))

(defn mask-path-leaf-with-items
  [relevant-items path]
  (update-in path [2] set/intersection relevant-items))

(defn query-index
  [index pred-clauses]
  (let [result-clauses         (filter-index index pred-clauses)
        relevant-items         (items-that-answer-all-conditions (map peek result-clauses) (count pred-clauses))
        cleaned-result-clauses (map #(mask-path-leaf-with-items relevant-items %) result-clauses)]
    (filter #(seq (peek %)) cleaned-result-clauses)))

;; NOTE: continue at "Table 10.7"

(defn single-index-query-plan
  [query-pred-clauses index-kw db]
  (let [db-index (core/index-at db index-kw)
        q-res (query-index db-index query-pred-clauses)]
    (bind-variables-to-query q-res db-index)))

(defn ^:private collapse-variable-vectors
  [accV v]
  (mapv #(when (= %1 %2) %1) accV v))

(defn index-of-joining-variable
  ^long [query-pred-clauses]
  (->> query-pred-clauses
       (mapv #(:db/variable (meta %)))
       (reduce collapse-variable-vectors)
       (keep-indexed #(when (variable? %2 false) %1))
       first))

(defn build-query-plan
  [query-pred-clauses]
  (let [join-index  (index-of-joining-variable query-pred-clauses)
        db-index-kw (case join-index 0 :AVET 1 :VEAT 2 :EAVT)]
    #(single-index-query-plan query-pred-clauses db-index-kw %)))
