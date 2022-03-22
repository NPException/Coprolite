(ns de.npcomplete.coprolite.util)

;; This function was ommited in the book. The implementation is taken from https://github.com/aosabook/500lines/blob/master/functionalDB/code/fdb/constructs.clj#L59
(defn collify
  [x]
  (if (coll? x) x [x]))

(defmacro >-
  "Like -> but threads x at the front of each form. Example:
  (>- m :a :b) expands to ((m :a) :b)"
  [x & forms]
  (loop [x x, forms forms]
    (if forms
      (let [form (first forms)
            threaded (if (seq? form)
                       (with-meta `(~x ~@form) (meta form))
                       (list x form))]
        (recur threaded (next forms)))
      x)))