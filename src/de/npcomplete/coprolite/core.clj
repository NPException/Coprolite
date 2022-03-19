(ns de.npcomplete.coprolite.core
  (:require [de.npcomplete.coprolite.util :refer [>-]])
  (:gen-class))

(defrecord Database [layers top-id curr-time])

(defrecord Layer [storage VAET AVET VEAT EAVT])

(defrecord Entity [id attributes])

(defn make-entity
  ([] (make-entity :db/no-id-yet))
  ([id] (Entity. id {})))


(defrecord Attribute [name value ts prev-ts])

(defn make-attribute
  "The type of the attribute may be either :string, :number, :boolean or :db/ref . If the type is :db/ref, the value is an id of another entity."
  [name value type
   & {:keys [cardinality] :or {cardinality :db/single}}]
  {:pre [(contains? #{:string :number :boolean :db/ref} type)
         (contains? #{:db/single :db/multiple} cardinality)]}
  (with-meta (Attribute. name value -1 -1)
    {:type type :cardinality cardinality}))

(defn add-attribute
  [entity attribute]
  (let [attr-id (keyword (:name attribute))]
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


(defn ref?
  [attr]
  (= :db/ref (:type (meta attr))))

(defn make-db
  ([] (make-db (InMemoryStorage.)))
  ([storage]
   (atom (Database.
           [(Layer. storage
              (make-index #(vector %3 %2 %1) #(vector %3 %2 %1) ref?) ;VAET
              (make-index #(vector %2 %3 %1) #(vector %3 %1 %2) any?) ;AVET
              (make-index #(vector %3 %1 %2) #(vector %2 %3 %1) any?) ;VEAT
              (make-index #(vector %1 %2 %3) #(vector %1 %2 %3) any?))] ;EAVT
           0 0))))


;; Basic accessor functions ;;

(defn entity-at
  (^Entity [db entity-id]
   (entity-at db entity-id (:curr-time db)))
  ;; NOTE: param order changed from tutorial
  (^Entity [db ent-id ts]
   (-> db :layers (>- ts) :storage (get-entity ent-id))))

(defn attribute-at
  (^Attribute [db entity-id attrib-name]
   (attribute-at db entity-id attrib-name (:curr-time db)))
  (^Attribute [db entity-id attrib-name ts]
   ((:attributes (entity-at db entity-id ts) {}) attrib-name)))

(defn value-of-at
  ([db entity-id attrib-name]
   (value-of-at db entity-id attrib-name (:curr-time db)))
  ([db entity-id attrib-name ts]
   (:value (attribute-at db entity-id attrib-name ts))))

(defn index-at
  ([db kind]
   (index-at db kind (:curr-time db)))
  ([^Database db kind ts]
   (kind ((:layers db) ts))))


(defn evolution-of
  "Returns all changes for an attribute over time, from most to least recent. Eager."
  [db entity-id attrib-name]
  (loop [res [], ts (:curr-time db)]
    (if (= -1 ts)
      res                                                   ;; tutorial returned `(reverse res)` instead
      (let [attr (attribute-at db entity-id attrib-name ts)]
        (recur (conj res [(:ts attr) (:value attr)]), (:prev-ts attr))))))

(defn lazy-evolution-of
  "Returns an iteration (sequable/reducible) of the change history of an attribute, from most to least recent."
  [db entity-id attrib-name]
  ;; TODO: check if this works as expected
  (iteration
    (fn [ts] (attribute-at db entity-id attrib-name ts))
    :initk (:curr-time db)
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
        ^long top-id (:top-id db)
        increased-id (inc top-id)
        entity'      (if new-entity? (assoc entity :id (keyword (str increased-id))) entity)
        next-top-id  (if new-entity? increased-id top-id)]
    [(update-creation-ts entity' (next-ts db)) next-top-id]))


;; NOTE: heavily modified from tutorial. restore to original if something's off.
(defn ^:private update-entry-in-index
  [index [k1 k2 update-value :as _path] operation]
  (update-in index [k1 k2] #(conj (or % #{}) update-value)))

;; This function was ommited in the book. The implementation is taken from https://github.com/aosabook/500lines/blob/master/functionalDB/code/fdb/constructs.clj#L59
(defn ^:private collify
  [x]
  (if (coll? x) x [x]))

(defn ^:private update-attribute-in-index
  [index ent-id attr-name target-val operation]
  (let [colled-target-val (collify target-val)
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
        relevant-attributes (filterv (usage-pred index) all-attributes)
        add-in-index-fn     (fn [ind attr]
                              (update-attribute-in-index ind entity-id (:name attr)
                                (:value attr)
                                :db/add))]
    (assoc layer index-name (reduce add-in-index-fn index relevant-attributes))))


(defn add-entity
  [db entity]
  (let [[fixed-entity next-top-id] (fix-new-entity db entity)
        layer-with-updated-storage (update (peek (:layers db)) :storage write-entity fixed-entity)
        new-layer                  (reduce
                                     #(add-entity-to-index fixed-entity %1 %2)
                                     layer-with-updated-storage
                                     indexes)]
    (-> (update db :layers conj new-layer)
        (assoc :top-id next-top-id))))

(defn add-entities
  [db entities]
  (reduce add-entity db entities))


;; TODO
(defn update-entity
  [db ent-id attibute-name value operation])


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

(defn ^:private remove-entry-from-index
  [index [k1 k2 val-to-remove :as _path]]
  (let [path-to-items   [k1 k2]
        old-entries-set (get-in index path-to-items)]
    (cond
      (not (contains? old-entries-set val-to-remove)) index ; the set of items does not contain the item to remove, => nothing to do here
      (= 1 (count old-entries-set)) (update index k1 dissoc k2) ; a path that splits at the second item - just remove the unneeded part of it
      :else (update-in index path-to-items disj val-to-remove))))

(defn ^:private remove-entries-from-index
  [ent-id operation index attr]
  (if (= operation :db/add)
    index
    (let [attr-name   (:name attr)
          datom-vals  (collify (:value attr))
          from-eav-fn (from-eav index)
          paths       (mapv #(from-eav-fn ent-id attr-name %) datom-vals)]
      (reduce remove-entry-from-index index paths))))

(defn ^:private remove-entity-from-index
  [entity layer index-name]
  (let [ent-id               (:id entity)
        index                (index-name layer)
        all-attributes       (vals (:attributes entity))
        relevant-attributes  (filterv (usage-pred index) all-attributes)
        remove-from-index-fn #(remove-entries-from-index ent-id :db/remove %1 %2)]
    (assoc layer index-name (reduce remove-from-index-fn index relevant-attributes))))


(defn remove-entity
  [db ent-id]
  (let [entity       (entity-at db ent-id)
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


(defn -main
  [& _args]
  (println "Hello, World!"))
