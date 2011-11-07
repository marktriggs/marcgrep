(ns marcgrep.predicates
  (:refer-clojure :exclude [contains?])
  (:use marcgrep.config
        [clojure.string :only [join]])
  (:import [org.marc4j.marc Record VariableField DataField ControlField
            Subfield]))

(defn matching-fields [^Record record fieldspec]
  (filter (fn [^VariableField field]
            (and
             (= (.getTag field) (:tag fieldspec))
             (or (re-matches (:controlfield-pattern @config) (:tag fieldspec))
                 (or (not (:ind1 fieldspec))
                     (.equals ^Character (.getIndicator1 ^DataField field)
                              ^Character (:ind1 fieldspec)))
                 (or (not (:ind2 fieldspec))
                     (.equals ^Character (.getIndicator2 ^DataField field)
                              ^Character (:ind2 fieldspec))))))
          (.getVariableFields record)))


(defn field-values [^Record record fieldspec]
  (if (nil? (:tag fieldspec))
    ;; match any field in the record
    (for [variable-field (.getVariableFields record)
          subfield (if (instance? ControlField variable-field)
                     [variable-field]
                     (.getSubfields variable-field))]
      (.getData subfield))

    (if (re-matches (:controlfield-pattern @config) (:tag fieldspec))
      (when (.getVariableField record (:tag fieldspec))
        [(.getData ^ControlField (.getVariableField record (:tag fieldspec)))])
      (when-let [fields (seq (matching-fields record fieldspec))]
        (map (fn [^DataField field]
               (let [subfield-list (if (:subfields fieldspec)
                                     (mapcat #(.getSubfields field %) (:subfields fieldspec))
                                     (.getSubfields field))]
                 (join " " (map #(.getData ^Subfield %) subfield-list))))
             fields)))))


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
              (field-values record fieldspec))))


(defn equals? [record fieldspec value]
  (some (fn [^String fv] (.equalsIgnoreCase fv value))
        (field-values record fieldspec)))


(defn not-equals? [record fieldspec value]
  (when-let [fields (field-values record fieldspec)]
    (not-any? (fn [^String fv] (.equalsIgnoreCase fv value))
              (field-values record fieldspec))))


(defn exists? [record fieldspec _]
  (field-values record fieldspec))


(def not-exists? (complement exists?))


(defn matches? [record fieldspec value]
  (some (fn [^String fv] (.matches (.matcher (re-pattern value)
                                             fv)))
        (field-values record fieldspec)))


(defn not-matches? [record fieldspec value]
  (when-let [fields (field-values record fieldspec)]
    (not-any? (fn [^String fv]
                (.matches (.matcher (re-pattern value)
                                    fv)))
              fields)))


(defn has-repeating-fields? [record fieldspec value]
  (> (count (matching-fields record fieldspec))
     1))


(defn does-not-repeat-field? [record fieldspec value]
  (= (count (matching-fields record fieldspec))
     1))
