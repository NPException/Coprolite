(ns de.npcomplete.coprolite.core
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
  (with-meta (Attribute. name value -1 -1) {:type type :cardinality cardinality}))

(defn add-attribute [entity attribute]
  (let [attr-id (keyword (:name attribute))]
    (assoc-in entity [:attributes attr-id] attribute)))


(defprotocol Storage
  (get-entity [storage e-id] )
  (write-entity [storage entity])
  (drop-entity [storage entity]))

(defrecord InMemoryStorage []
  Storage
  (get-entity [storage e-id]
    (e-id storage))
  (write-entity [storage entity]
    (assoc storage (:id entity) entity))
  (drop-entity [storage entity]
    (dissoc storage (:id entity))))


(defn -main
  [& _args]
  (println "Hello, World!"))
