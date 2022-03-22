(ns de.npcomplete.coprolite.graph
  (:require [de.npcomplete.coprolite.core :as cp]
            [de.npcomplete.coprolite.util :as u]))

;; Graph traversal

(defn incoming-refs
  [db ts ent-id & ref-names]
  (if-not ent-id
    #{}
    (let [vaet               (cp/index-at db :VAET ts)
          all-attributes-map (vaet ent-id)
          filtered-map       (if ref-names
                               (select-keys all-attributes-map ref-names)
                               all-attributes-map)]
      (into #{} cat (vals filtered-map)))))

;; NOTE: the tutorial uses a vector as a return type here, which feels inconsistent, so I changed it.
(defn outgoing-refs
  [db ts ent-id & ref-names]
  (if-not ent-id
    #{}
    (let [attrib-filter-fn (if ref-names #(select-keys % ref-names) identity)]
      (->> (cp/entity-at db ent-id ts)
           :attributes
           attrib-filter-fn
           vals
           (into #{} (comp (filter cp/ref?)
                           (map :value)
                           (mapcat u/collify)))))))
