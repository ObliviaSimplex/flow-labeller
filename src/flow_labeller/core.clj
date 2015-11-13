(ns flow-labeller.core
  (:gen-class))

;; =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
;; Generates a list of service labels for the flows itemized by flowtbag,
;; based on port numbers and knowledge of the IP protocol (TCP/UDP).
;; =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

(require '[clojure.string :as str])

;; Some handy numerical constants:
(def tcp 6)
(def udp 17)
(def proto-index 4) ;; which is also the index at which we'll crop the flow
 
(defn csv->map [csvpath]
  (let [txt (str/split (slurp csvpath) #"[\n,]")]
    (apply hash-map txt)))
    
(def udp-portmap
  (csv->map (pathname "udp-portlabels.txt")))

(def tcp-portmap
  (csv->map (pathname "tcp-portlabels.txt")))

(defn get-service-list []
  (distinct (concat (vals tcp-portmap) (vals udp-portmap))))

(defn flows
  "Grabs flows from flow file."
  [flow-filename]
  (let [flowvec (str/split (slurp flow-filename) #"\n")]
   (map (fn [x] (str/split x #",")) flowvec)))

(def p1 1)
(def p2 3)

(defn port-vectorize [the-flows]
  (map
   (fn [x] (vector (min (bigint (nth x p1)) (bigint (nth x p2)))
                       (bigint (nth x proto-index))))
   the-flows))

(defn map-port
  "Maps port numbers to their corresponding services, after checking to see 
  whether their flow is labelled as TCP or UDP."
  [pair]
  (if (= (pair 1) tcp)        ;; check to see if tcp service or udp
    (tcp-portmap (str (pair 0)))
    (udp-portmap (str (pair 0)))))

(defn port-mapper [pvec]
  (map map-port pvec))

(defn arff-maker 
  "Generates an arff file that is readable by Weka."
  [flow-filename tidy-attributes]
  ;; if tidy-attributes = 1, then list only used attrs in the header
  (let [the-flows (flows flow-filename) labels (port-mapper (port-vectorize the-flows))]
    (let [processed-flows
          (map (fn [x y] (conj (subvec x (+ proto-index 1)) y)) the-flows labels)]
      (do
        ;; Relation section
        (print "@RELATION application\n\n")
        ;; Attributes section
        ;; First, the numerical fields, to which we give boring names:
        (loop [n 1, fields (first processed-flows)]
          (printf "@ATTRIBUTE f%d\tNUMERIC\n" n)
          (when (seq (rest (rest fields)))
            (recur (inc n) (rest fields))))
        ;; Now print the list of services as a set of nominal values
        (print "@ATTRIBUTE service\t{")
        (loop [service-list (if tidy-attributes
                              (distinct labels)
                              (get-service-list))]
      (print (first service-list))
      (when (seq (rest service-list))
        (print ",")
        (recur (rest service-list))))
    (print "}\n\n")
    ;; Data section
      (println "@DATA")
      (doseq [labelled-flow processed-flows]
        (println (apply str (interpose "," labelled-flow))))))))

(defn efs-prepper
  "Formats the flows so that they can be processed by EFS."
  [flowpath]
  (let [the-flows (flows flowpath)]
    (let [processed-flows
          (map (fn [x y] (conj x y))
               (map (fn [x] (subvec x proto-index)) the-flows)
               (map (fn [x] (x 0)) (port-vectorize the-flows)))]
      (doseq [pf processed-flows]
        (println (apply str (interpose "," pf)))))))
  
(defn -main
  "Prints a list of labels for the flow provided."
  [& args]
  (let [flowpath (first args)]
    (case (second args)
      "arff"
      (let [tidy-opt (if (= "tidy" (second (rest args))) true nil)]
        (arff-maker flowpath tidy-opt))
      "efs"
      (efs-prepper flowpath)
      (println "Usage: java -jar <path to flow file> <format> ['tidy']\nWhere format is arff or efs."))))




