(ns flow-labeller.core
  (:gen-class))

;; =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
;; Generates a list of service labels for the flows itemized by flowtbag,
;; based on port numbers and knowledge of the IP protocol (TCP/UDP).
;; =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

(require '[clojure.string :as str])

;; Some handy numerical constants:
(def tcp "6")
(def udp "17")
(def proto-index 4) ;; which is also the index at which we'll crop the flow

(def p1 1)
(def p2 3)

(def DEFAULTPATH "/home/oblivia/cs/6706-network-design+management/assignments/A2/DARPA/outside.flow") ;; for testing

;;
(def service-map
  (let [servvecs
        (let [servlist (str/split (slurp "/etc/services") #"\n")]
          (map (fn [z] (conj  [(str/split (second z) #"/")] (first z)))
               (map (fn [y] (str/split y #"\s+"))
                    (filter (fn [x] (not (= "" x))) 
                            (map (fn [x] (str/replace x #"#.*" "")) 
                                 servlist)))))]
    (into {} servvecs)))

(defn serv-key
  "Gets service-maps key corresponding to a flow."
  [flow]
  (let [proto (if (= (flow proto-index) tcp) "tcp" "udp")
        port (str (min (bigint (nth flow p1)) (bigint (nth flow p2))))]
    [port proto]))

(defn lookup-service
  "Takes a flow as argument, and returns the name of its service,
  as a string."
  [flow]
  (service-map (serv-key flow)))

(defn service-lister
  [the-flows]
  (map lookup-service the-flows))

(defn flows
  "Grabs flows from flow file."
  [flow-filename]
  (let [flowvec (str/split (slurp flow-filename) #"\n")]
   (map (fn [x] (str/split x #",")) flowvec)))

(defn arff-maker 
  "Generates an arff file that is readable by Weka."
  [flow-filename tidy-attributes]
  ;; if tidy-attributes = 1, then list only used attrs in the header
  (let [the-flows (flows flow-filename)
        labels (service-lister the-flows)]
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
                              (vals service-map))]
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
               (map (fn [x] (min (bigint (x p1))
                                (bigint (x p2)))) the-flows)) ]
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





