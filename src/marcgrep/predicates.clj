(ns marcgrep.predicates
  (:refer-clojure :exclude [contains?])
  (:use marcgrep.config
        [clojure.string :only [join]])
  (:import [org.marc4j.marc Record VariableField DataField ControlField
            Subfield]))

(defn matching-fields [^Record record fieldspec]
  (filter (fn [^VariableField field]
            (and
             (.startsWith (.getTag field) (:tag fieldspec))
             (or (instance? ControlField field)
                 (or (not (:ind1 fieldspec))
                     (.equals ^Character (.getIndicator1 ^DataField field)
                              ^Character (:ind1 fieldspec)))
                 (or (not (:ind2 fieldspec))
                     (.equals ^Character (.getIndicator2 ^DataField field)
                              ^Character (:ind2 fieldspec))))))
          (.getVariableFields record)))


(defn field-values [^Record record fieldspec]
  (when-let [fields (seq (matching-fields record fieldspec))]
    (map (fn [^VariableField field]
           (if (instance? DataField field)
             (let [field ^DataField field
                   subfield-list (if (:subfields fieldspec)
                                   (mapcat #(.getSubfields field %) (:subfields fieldspec))
                                   (.getSubfields field))]
               (join " " (map #(.getData ^Subfield %) subfield-list)))
             (.getData ^ControlField field)))
         fields)))


(defn contains-keyword? [record fieldspec ^String value]
  (let [pattern (re-pattern (format "(?i)\\b\\Q%s\\E\\b"
                                    value))]
    (some (fn [^String fv] (.find (.matcher pattern fv)))
          (field-values record fieldspec))))


(defn does-not-contain-keyword? [record fieldspec ^String value]
  (when-let [fields (field-values record fieldspec)]
    (let [pattern (re-pattern (format "(?i)\\b\\Q%s\\E\\b"
                                      value))]
      (not-any? (fn [^String fv] (.find (.matcher pattern fv)))
                fields))))


(defn contains? [record fieldspec ^String value]
  (some (fn [^String fv]
          (>= (.indexOf (.toLowerCase fv)
                        value)
              0))
        (field-values record fieldspec)))


(defn does-not-contain? [record fieldspec ^String value]
  (when-let [fields (field-values record fieldspec)]
    (not-any? (fn [^String fv]
                (>= (.indexOf (.toLowerCase fv)
                              value)
                    0))
              fields)))


(defn equals? [record fieldspec value]
  (some (fn [^String fv] (.equalsIgnoreCase fv value))
        (field-values record fieldspec)))


(defn not-equals? [record fieldspec value]
  (when-let [fields (field-values record fieldspec)]
    (not-any? (fn [^String fv] (.equalsIgnoreCase fv value))
              fields)))


(defn exists? [record fieldspec _]
  (field-values record fieldspec))


(def not-exists? (complement exists?))


(defn matches? [record fieldspec value]
  (let [pattern (re-pattern value)]
    (some (fn [^String fv] (.find (.matcher pattern fv)))
          (field-values record fieldspec))))


(defn not-matches? [record fieldspec value]
  (when-let [fields (field-values record fieldspec)]
    (let [pattern (re-pattern value)]
      (not-any? (fn [^String fv]
                  (.find (.matcher pattern fv)))
                fields))))


(defn has-repeating-fields? [record fieldspec value]
  (> (count (matching-fields record fieldspec))
     1))


(defn does-not-repeat-field? [record fieldspec value]
  (= (count (matching-fields record fieldspec))
     1))
