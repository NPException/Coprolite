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
  [name value type
   & {:keys [cardinality] :or {cardinality :db/single}}]
  {:pre [(contains? #{:db/single :db/multiple} cardinality)]}
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

(defn indexes
  "Returns which indexes are available on every database `Layer`."
  []
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


(defn entity-at
  (^Entity [db ent-id]
   (entity-at db ent-id (:curr-time db)))
  ;; NOTE: param order changed from tutorial
  (^Entity [db ent-id ts]
   (-> db :layers (>- ts) :storage (get-entity ent-id))))

(defn attribute-at
  (^Attribute [db ent-id attrib-name]
   (attribute-at db ent-id attrib-name (:curr-time db)))
  (^Attribute [db ent-id attrib-name ts]
   ((:attributes (entity-at db ent-id ts) {}) attrib-name)))

(defn value-of-at
  ([db ent-id attrib-name]
   (value-of-at db ent-id attrib-name (:curr-time db)))
  ([db ent-id attrib-name ts]
   (:value (attribute-at db ent-id attrib-name ts))))

(defn index-at
  ([db kind]
   (index-at db kind (:curr-time db)))
  ([^Database db kind ts]
   (kind ((:layers db) ts))))


(defn evolution-of
  "Returns all changes for an attribute over time, from most to least recent. Eager."
  [db ent-id attrib-name]
  (loop [res [], ts (:curr-time db)]
    (if (= -1 ts)
      res                                                   ;; tutorial returned `(reverse res)` instead
      (let [attr (attribute-at db ent-id attrib-name ts)]
        (recur (conj res [(:ts attr) (:value attr)]), (:prev-ts attr))))))

(defn lazy-evolution-of
  "Returns an iteration (sequable/reducible) of the change history of an attribute, from most to least recent."
  [db ent-id attrib-name]
  ;; TODO: check if this works as expected
  (iteration
    (fn [ts] (attribute-at db ent-id attrib-name ts))
    :initk (:curr-time db)
    :kf (fn [attrib] (let [^long ts (:prev-ts attrib)]
                       (if (= ts -1) nil ts)))
    :vf (fn [attrib] [(:ts attrib) (:value attrib)])))


(defn -main
  [& _args]
  (println "Hello, World!"))
