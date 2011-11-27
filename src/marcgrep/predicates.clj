(ns marcgrep.predicates
  (:refer-clojure :exclude [contains?])
  (:use marcgrep.config
        [clojure.string :only [join]])
  (:import [org.marc4j.marc Record VariableField DataField ControlField
            Subfield]
           [java.util.regex Pattern]))


(defn matching-fields [^Record record fieldspec]
  (filter (fn [^VariableField field]
            (and
             (.startsWith (.getTag field) (:tag fieldspec))
             (or (instance? ControlField field)
                 (and (or (not (:ind1 fieldspec))
                          (re-matches (:ind1 fieldspec)
                                      (str (.getIndicator1 ^DataField field))))
                      (or (not (:ind2 fieldspec))
                          (re-matches (:ind2 fieldspec)
                                      (str (.getIndicator2 ^DataField field))))))))
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


(defn keyword-regexp [keyword]
  (let [boundary "(^|$|[\\p{Space}\\p{Punct}&&[^']])"]
    (re-pattern (str "(?i)" boundary "\\Q" keyword "\\E" boundary) )))


(defn contains-keyword? [record fieldspec ^String value]
  (let [^Pattern pattern (keyword-regexp value)]
    (some (fn [^String fv] (.find (.matcher pattern fv)))
          (field-values record fieldspec))))


(defn does-not-contain-keyword? [record fieldspec ^String value]
  (when-let [fields (field-values record fieldspec)]
    (let [^Pattern pattern (keyword-regexp value)]
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


(defn has-repeating-subfield? [record fieldspec value]
  (let [subfield-code (first value)]
    (some (fn [^VariableField field]
            (when (instance? DataField field)
              (> (count (filter #(= (.getCode %) subfield-code)
                                (.getSubfields ^DataField field)))
                 1)))
          (matching-fields record fieldspec))))


(defn does-not-repeat-subfield? [record fieldspec value]
  (let [subfield-code (first value)]
    (every? (fn [^VariableField field]
              (when (instance? DataField field)
                (= (count (filter #(= (.getCode %) subfield-code)
                                  (.getSubfields ^DataField field)))
                   1)))
            (matching-fields record fieldspec))))

