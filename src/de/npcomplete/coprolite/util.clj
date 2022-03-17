(ns de.npcomplete.coprolite.util)

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