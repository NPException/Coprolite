(ns de.npcomplete.coprolite.core
  (:require [clojure.set :as set]
            [de.npcomplete.coprolite.util :as u :refer [>-]])
  (:gen-class))

(defrecord Database [layers ^long curr-time])

(defrecord Layer [storage ^long top-id VAET AVET VEAT EAVT])

(defrecord Entity [id attributes])

(defn make-entity
  ([] (make-entity :db/no-id-yet))
  ([id]
   {:pre [(keyword? id)]}
   (Entity. id {})))


(defrecord Attribute [name value ^long ts ^long prev-ts])

(defn ^:private valid-attribute-value?
  [value]
  (or (not (coll? value))
      (set? value)))

(defn make-attribute
  "If the value is a keyword of a number (e.g. ':123'), it is a reference/id of another entity.
  If the attribute should support a collection of values (have `multiple` cardinality), they must be passed as a set."
  [name value]
  {:pre [(keyword? name)
         (valid-attribute-value? value)]}
  (Attribute. name value -1 -1))

(defn add-attribute
  [entity attribute]
  (let [attr-id (:name attribute)]
    (assoc-in entity [:attributes attr-id] attribute)))


(defprotocol Storage
  (get-entity [this e-id])
  (write-entity [this entity])
  (drop-entity [this entity]))

(defrecord InMemoryStorage []
  Storage
  (get-entity [this e-id]
    (e-id this))
  (write-entity [this entity]
    (assoc this (:id entity) entity))
  (drop-entity [this entity]
    (dissoc this (:id entity))))


(defn make-index
  [from-eav to-eav usage-pred]
  (with-meta {} {:from-eav from-eav :to-eav to-eav :usage-pred usage-pred}))

(defn from-eav
  [index]
  (:from-eav (meta index)))

(defn to-eav
  [index]
  (:to-eav (meta index)))

(defn usage-pred
  [index]
  (:usage-pred (meta index)))

(def indexes
  "The indexes available on every database layer."
  [:VAET :AVET :VEAT :EAVT])


(defn single?
  [attribute]
  (not (set? (:value attribute))))

(defn ref?
  [attribute]
  (let [value (:value attribute)]
    (and (keyword? value)
         (every? (fn [c]
                   (let [i (int c)]
                     (and (<= 48 i) (<= i 57))))
           (name value)))))

(defn make-db
  ([] (make-db (InMemoryStorage.)))
  ([storage]
   (atom (Database.
           [(Layer. storage 0
              (make-index #(vector %3 %2 %1) #(vector %3 %2 %1) ref?) ;VAET
              (make-index #(vector %2 %3 %1) #(vector %3 %1 %2) any?) ;AVET
              (make-index #(vector %3 %1 %2) #(vector %2 %3 %1) any?) ;VEAT
              (make-index #(vector %1 %2 %3) #(vector %1 %2 %3) any?))] ;EAVT
           0))))

(defn ^:private current-layer-ts
  ^long [^Database db]
  (-> db :layers count dec))

(defn db-at
  [^Database db ^long ts]
  (-> db
      (update :layers subvec 0 (inc ts))
      (assoc :curr-time ts)))

(defn prev-db
  [^Database db]
  (db-at db (dec (current-layer-ts db))))


;; Basic accessor functions ;;

(defn entity-at
  (^Entity [db entity-id]
   (entity-at db entity-id (current-layer-ts db)))
  ;; NOTE: param order changed from tutorial
  (^Entity [db ent-id ts]
   (-> db :layers (>- ts) :storage (get-entity ent-id))))

(defn attribute-at
  (^Attribute [db entity-id attrib-name]
   (attribute-at db entity-id attrib-name (current-layer-ts db)))
  (^Attribute [db entity-id attrib-name ts]
   ((:attributes (entity-at db entity-id ts) {}) attrib-name)))

(defn value-of-at
  ([db entity-id attrib-name]
   (value-of-at db entity-id attrib-name (current-layer-ts db)))
  ([db entity-id attrib-name ts]
   (:value (attribute-at db entity-id attrib-name ts))))

(defn index-at
  ([db kind]
   (index-at db kind (current-layer-ts db)))
  ([^Database db kind ts]
   (kind ((:layers db) ts))))


(defn evolution-of
  "Returns all changes for an attribute over time, from least to most recent. Eager."
  [db entity-id attrib-name]
  (loop [res (transient []), ts (current-layer-ts db)]
    (if (= -1 ts)
      (rseq (persistent! res))
      (let [attr (attribute-at db entity-id attrib-name ts)]
        (recur (conj! res [(:ts attr) (:value attr)]), (long (:prev-ts attr)))))))

(defn evolution-trace
  "Returns an iteration (sequable/reducible) of the change history of an attribute, from most to least recent."
  [db entity-id attrib-name]
  (iteration
    (fn [ts] (attribute-at db entity-id attrib-name ts))
    :initk (current-layer-ts db)
    :kf (fn [attrib] (let [^long ts (:prev-ts attrib)]
                       (if (= ts -1) nil ts)))
    :vf (fn [attrib] [(:ts attrib) (:value attrib)])))


;; Data behaviour and lifecycle ;;

(defn ^:private next-ts
  [db]
  (inc ^long (:curr-time db)))

(defn ^:private update-creation-ts
  [entity new-ts]
  (reduce
    #(assoc-in %1 [:attributes %2 :ts] new-ts)
    entity
    (keys (:attributes entity))))

;; NOTE: heavily modified from tutorial. restore to original if something's off.
(defn ^:private fix-new-entity
  [db entity]
  (let [new-entity?  (= :db/no-id-yet (:id entity))
        ^long top-id (-> db :layers peek :top-id)
        increased-id (inc top-id)
        entity'      (if new-entity? (assoc entity :id (keyword (str increased-id))) entity)
        next-top-id  (if new-entity? increased-id top-id)]
    [(update-creation-ts entity' (next-ts db)) next-top-id]))


;; NOTE: heavily modified from tutorial. restore to original if something's off.
(defn ^:private update-entry-in-index
  [index [k1 k2 update-value :as _path] _operation]         ;; TODO: remove unused `operation` parameter (or use it for logging?)
  (update-in index [k1 k2] #(conj (or % #{}) update-value)))

(defn ^:private update-attribute-in-index
  [index ent-id attr-name target-val operation]
  (let [colled-target-val (u/collify target-val)
        from-eav-fn       (from-eav index)
        update-entry-fn   (fn [index vl]
                            (update-entry-in-index
                              index
                              (from-eav-fn ent-id attr-name vl)
                              operation))]
    (reduce update-entry-fn index colled-target-val)))

(defn ^:private add-entity-to-index
  [entity layer index-name]
  (let [entity-id           (:id entity)
        index               (index-name layer)
        all-attributes      (vals (:attributes entity))
        relevant-attributes (eduction (filter (usage-pred index)) all-attributes)
        add-in-index-fn     (fn [ind attr]
                              (update-attribute-in-index ind entity-id (:name attr)
                                (:value attr)
                                :db/add))]
    (assoc layer index-name (reduce add-in-index-fn index relevant-attributes))))


(defn add-entity
  [db entity]
  (let [[fixed-entity next-top-id] (fix-new-entity db entity)
        layer-with-updated-storage (-> (peek (:layers db))
                                       (update :storage write-entity fixed-entity)
                                       (assoc :top-id next-top-id))
        new-layer                  (reduce
                                     #(add-entity-to-index fixed-entity %1 %2)
                                     layer-with-updated-storage
                                     indexes)]
    (update db :layers conj new-layer)))

(defn add-entities
  [db entities]
  (reduce add-entity db entities))


(defn ^:private update-attribute-modification-time
  [attribute new-ts]
  (-> (assoc attribute :ts new-ts)
      (assoc :prev-ts (:ts attribute))))

(defn ^:private setify
  [x]
  (if (set? x) x #{x}))

(defn ^:private update-attribute-value
  [attribute value operation]
  (cond
    (= :db/reset-to operation)
    (if (single? attribute)
      (assoc attribute :value value)
      (assoc attribute :value (setify value)))
    (= :db/remove operation)
    (if (single? attribute)
      (assoc attribute :value nil)
      (assoc attribute :value (set/difference (:value attribute) (setify value))))
    (= :db/add operation)
    (assoc attribute :value (set/union (:value attribute) (setify value)))))

(defn ^:private update-attribute
  [attribute new-val new-ts operation]
  {:pre [(or (some? new-val) (= operation :db/remove))
         (valid-attribute-value? new-val)
         (if (single? attribute)
           (and (not (set? new-val)) (contains? #{:db/reset-to :db/remove} operation))
           (contains? #{:db/reset-to :db/add :db/remove} operation))]}
  (-> attribute
      (update-attribute-modification-time new-ts)
      (update-attribute-value new-val operation)))

(defn ^:private remove-entry-from-index
  [index [k1 k2 val-to-remove :as _path]]
  (let [old-entries-set ((index k1 {}) k2)]
    (cond
      ; the set of items does not contain the item to remove, => nothing to do here
      (not (contains? old-entries-set val-to-remove))
      index
      ; a path that splits at the second item - just remove the unneeded part of it
      (= 1 (count old-entries-set))
      (let [updated-k1-map (dissoc (index k1) k2)]
        (if (empty? updated-k1-map)
          (dissoc index k1)
          (assoc index k1 updated-k1-map)))
      :else
      (update-in index [k1 k2] disj val-to-remove))))

(defn ^:private remove-entries-from-index
  [ent-id operation index attr]
  (if (= operation :db/add)
    index
    (let [attr-name   (:name attr)
          datom-vals  (u/collify (:value attr))
          from-eav-fn (from-eav index)
          paths       (eduction (map #(from-eav-fn ent-id attr-name %)) datom-vals)]
      (reduce remove-entry-from-index index paths))))

(defn ^:private update-index
  [ent-id old-attribute target-val operation layer index-name]
  (if-not ((usage-pred (index-name layer)) old-attribute)
    layer
    (let [index         (index-name layer)
          cleaned-index (remove-entries-from-index ent-id operation index old-attribute)
          updated-index (if (= operation :db/remove)
                          cleaned-index
                          (update-attribute-in-index cleaned-index ent-id (:name old-attribute) target-val operation))]
      (assoc layer index-name updated-index))))

(defn ^:private update-layer
  [layer ent-id old-attribute updated-attribute new-val operation]
  (let [storage        (:storage layer)
        new-layer      (reduce
                         #(update-index ent-id old-attribute new-val operation %1 %2)
                         layer
                         indexes)
        updated-entity (assoc-in (get-entity storage ent-id)
                         [:attributes (:name updated-attribute)]
                         updated-attribute)]
    (assoc new-layer :storage (write-entity storage updated-entity))))

;; TODO: add the possiblity to add a new attribute to an existing entity.
(defn update-entity
  ([db ent-id attribute-name new-val]
   (update-entity db ent-id attribute-name new-val :db/reset-to))
  ([db ent-id attribute-name new-val operation]
   (let [update-ts         (next-ts db)
         layer             (peek (:layers db))
         entity            (-> layer :storage (get-entity ent-id))
         ;; TODO: make new attribute if it doesn't exist yet. Disambiguate the new attribute's cardinality by using different operation keywords for multi and single cardinality attributes.
         attribute         ((:attributes entity) attribute-name)
         updated-attribute (update-attribute attribute new-val update-ts operation)]
     ;; return `db` unchanged if attribute value didn't change
     (if (= (:value attribute) (:value updated-attribute))
       db
       (let [fully-updated-layer (update-layer layer ent-id attribute updated-attribute new-val operation)]
         (update db :layers conj fully-updated-layer))))))


(defn ^:private reffing-to
  [ent-id layer]
  (let [vaet (:VAET layer)]
    (for [[attribute-name reffing-set] (ent-id vaet)
          reffing reffing-set]
      [reffing attribute-name])))

(defn ^:private remove-back-refs
  [db ent-id layer]
  (let [reffing-datoms (reffing-to ent-id layer)
        remove-fn      (fn [db [e a]]
                         (update-entity db e a ent-id :db/remove))
        clean-db       (reduce remove-fn db reffing-datoms)]
    (peek (:layers clean-db))))

(defn ^:private remove-entity-from-index
  [entity layer index-name]
  (let [ent-id               (:id entity)
        index                (index-name layer)
        all-attributes       (vals (:attributes entity))
        relevant-attributes  (eduction (filter (usage-pred index)) all-attributes)
        remove-from-index-fn #(remove-entries-from-index ent-id :db/remove %1 %2)]
    (assoc layer index-name (reduce remove-from-index-fn index relevant-attributes))))


(defn remove-entity
  [db ent-id]
  (let [entity       (entity-at db ent-id)                  ;; TODO: just return `db` unchanged when entity isn't found
        layer        (remove-back-refs db ent-id (peek (:layers db)))
        no-ref-layer (update layer :VAET dissoc ent-id)
        no-ent-layer (update no-ref-layer :storage drop-entity entity)
        new-layer    (reduce
                       #(remove-entity-from-index entity %1 %2)
                       no-ent-layer
                       indexes)]
    (update db :layers conj new-layer)))

(defn remove-entities
  [db ent-ids]
  (reduce remove-entity db ent-ids))


;; Transaction handling ;;

(defn transact-on-db
  [initial-db tx-fns]
  (loop [[tx-fn & remaining-tx-fns] tx-fns
         transacted initial-db]
    (if tx-fn
      (recur remaining-tx-fns (tx-fn transacted))
      (-> initial-db
          (update :layers conj (peek (:layers transacted)))
          (assoc :curr-time (next-ts initial-db))))))

(defmacro _transact
  [db op & txs]
  (when txs
    (loop [[[tx-f & tx-args :as tx] & remaining-txs] txs
           accum-txs []]
      (if tx
        (recur remaining-txs (conj accum-txs `(fn [db#] (~tx-f db# ~@tx-args))))
        (list op db `transact-on-db accum-txs)))))

(defmacro transact
  [db-conn & txs]
  `(_transact ~db-conn swap! ~@txs))

(defmacro what-if
  [db & ops]
  `(_transact ~db (fn [db# f# txs#] (f# db# txs#)) ~@ops))


(defn -main
  [& _args]
  (println "Hello, World!"))


(comment

  (defn mapify [x]
    (clojure.walk/postwalk
      #(if (record? %) (into {} %) %)
      x))

  (mapify @db)

  (do
    (def db (make-db))

    (def dirk (-> (make-entity)
                  (add-attribute (make-attribute :first-name "Dirk"))
                  (add-attribute (make-attribute :last-name "Wetzel"))
                  (add-attribute (make-attribute :age 35))
                  (add-attribute (make-attribute :hobbies #{"Games" "Movies"}))))

    (transact db
      (add-entity dirk))

    (def calisto (-> (make-entity)
                     (add-attribute (make-attribute :name "Calisto"))
                     (add-attribute (make-attribute :breed "Siam-Mix"))
                     (add-attribute (make-attribute :species "Cat"))
                     (add-attribute (make-attribute :owner :1))))

    (def lumah (-> (make-entity)
                   (add-attribute (make-attribute :name "Lumah"))
                   (add-attribute (make-attribute :breed "EKH"))
                   (add-attribute (make-attribute :species "Cat"))
                   (add-attribute (make-attribute :owner :1))))

    (transact db
      (add-entity calisto)
      (add-entity lumah))

    (transact db
      (update-entity :1 :age 36)
      (update-entity :1 :hobbies #{"VR"} :db/add))

    nil)

  ;
  )
