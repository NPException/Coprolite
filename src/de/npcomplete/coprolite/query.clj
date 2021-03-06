(ns de.npcomplete.coprolite.query
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [de.npcomplete.coprolite.core :as core]
            [de.npcomplete.coprolite.util :as u]))

;; TODO (for later): consider moving away from macros for queries, so that they can be built dynamically at runtime

(defn ^:private variable?
  "A predicate that accepts a string and checks whether it describes a datalog variable (either starts with ? or it is _)"
  ([x] (variable? x true))
  ([x accept-underscore?]
   (and (symbol? x)
        (or (and accept-underscore? (= x '_))
            (str/starts-with? (name x) "?")))))

(defn ^:private clause-term-expr
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


(defmacro symbol-col-to-set
  [coll]
  (set coll))


;; query planning

(defn ^:private filter-index
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

(defn ^:private items-that-answer-all-conditions
  [index result-clauses ^long num-of-conditions]
  ;; debug eval: (mapv (juxt identity (comp :db/variable meta)) (doall result-clauses))
  ;; TODO: remove `duplicate` results (where the path to the result item is the same if part of the path is a variable)
  ;;       could try to replace the path parts with the variables and then throw the modified result-clauses in a set first
  (->> (map peek result-clauses)                            ; take the sets of result items
       (reduce into [])                                     ; reduce all the sets into one vector
       (frequencies)                                        ; count for each item in how many sets it was in
       (into #{}
         (keep (fn [[item ^long n]]
                 (when (>= n num-of-conditions)             ; items that answered all conditions
                   item))))))

(defn ^:private mask-path-leaf-with-items
  [relevant-items path]
  (update-in path [2] set/intersection relevant-items))

(defn ^:private query-index
  [index pred-clauses]
  (let [result-clauses         (filter-index index pred-clauses)
        relevant-items         (items-that-answer-all-conditions index result-clauses (count pred-clauses))
        cleaned-result-clauses (map #(mask-path-leaf-with-items relevant-items %) result-clauses)]
    (filter #(seq (peek %)) cleaned-result-clauses)))

(defn ^:private combine-path-and-variables
  [from-eav-fn [p1 p2 p3 :as path]]
  (let [[var1 var2 var3] (apply from-eav-fn (:db/variable (meta path)))] ;; reorder the variables to match our path
    (mapv vector
      (repeat var1) (repeat p1)
      (repeat var2) (repeat p2)
      (repeat var3) p3)))

(defn ^:private bind-variables-to-query
  [q-result index]
  (let [from-eav     (core/from-eav index)
        to-eav       (core/to-eav index)
        order-to-eav (fn [[v1 p1 v2 p2 v3 p3]]
                       (to-eav [v1 p1] [v2 p2] [v3 p3]))]
    (->> q-result
         (eduction (comp (mapcat #(combine-path-and-variables from-eav %))
                         (map order-to-eav)))
         (reduce #(assoc-in %1 (pop %2) (peek %2)) {}))))

(defn ^:private single-index-query-plan
  [query-pred-clauses index-kw db]
  (let [db-index (core/index-at db index-kw)
        q-result (query-index db-index query-pred-clauses)]
    (bind-variables-to-query q-result db-index)))

(defn ^:private collapse-variable-vectors
  [accV v]
  (mapv #(when (= %1 %2) %1) accV v))

(defn ^:private index-of-joining-variable
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

(defn ^:private resultify-bind-pair
  [needed-vars accum [var _ :as pair]]
  (if (contains? needed-vars var)
    (conj accum pair)
    accum))

(defn resultify-av-pair
  [needed-vars accum-res av-pair]
  (reduce #(resultify-bind-pair needed-vars %1 %2) accum-res av-pair))

(defn ^:private locate-vars-in-query-res
  [needed-vars [entity-pair av-map :as _query-result]]
  (let [entity-result (resultify-bind-pair needed-vars [] entity-pair)]
    (mapv #(resultify-av-pair needed-vars entity-result %) av-map)))

(defn unify
  [binded-res-col needed-vars]
  (mapv #(locate-vars-in-query-res needed-vars %) binded-res-col))


(comment

  (def plan (build-query-plan (q-clauses-to-pred-clauses [[?e :species "Cat"]
                                                          [?e :breed ?breed]])))

  (def plan (build-query-plan (q-clauses-to-pred-clauses [[?e :species "Cat"]
                                                          [?e ?what "EKH"]
                                                          [?e :breed ?breed]])))

  ;; FIXME: This should only return cats, but somehow returns all entities because of the second clause.
  ;;        The bug is probably in `items-that-answer-all-conditions`
  (def plan (build-query-plan (q-clauses-to-pred-clauses [[?e :species "Cat"]
                                                          [?e ?what ?any]
                                                          [?e :breed ?breed]])))

  ;; FIXME: This one should return nothing, but returns entity :1 (Dirk).
  ;;        The reason is that `items-that-answer-all-conditions` receives 3 result items
  ;;        for entity :1, one for each value in the :hobies attribute. Because of that it is
  ;;        counted 3 times by `frequencies`, and hence often enough to be assumed "matches all clauses".
  ;;        I could try to somehow
  (def plan (build-query-plan (q-clauses-to-pred-clauses [[?e :species "Cat"]
                                                          [?e :hobbies ?any]])))

  (plan @core/db)

  (unify (plan @core/db) '#{?e ?breed ?what ?any})
  ;
  )