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

(def DEFAULTPATH "/home/oblivia/cs/6706-network-design+management/assignments/A2/DARPA/sample_data01.flow") ;; for testing

;; Slups up /etc/services and turns it into a usable map associating
;; [port ip_protocol] string pairs with service names. 
(def service-map
  (let [servvecs
        (let [servlist (str/split (slurp "/etc/services") #"\n")]
          (map (fn [z] (conj  [(str/split (second z) #"/")] (first z)))
               (map (fn [y] (str/split y #"\s+"))
                    (filter (partial not= "") 
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
    (shuffle (map (fn [x] (str/split x #",")) flowvec)))) ;; added a shuffle, for luck

(defn writeout
  [filename & s]
  (doseq [item s]
    (spit filename (str item) :append true)))

(defn NOP2 [number somelist]
  somelist)
                                        ;
                                        ;(map (fn [x] (spit filename x :append true)) s))

;; pretty inefficient to be opening and closing the file with each spit.
;; figure out how to keep it open and close it at the end.

(defn arff-maker 
  "Generates an arff file that is readable by Weka."
  [flow-filename tidy-attributes rat]
  ;; if tidy-attributes = 1, then list only used attrs in the header
  (let [the-flows (flows flow-filename)
        labels (service-lister the-flows)]
    (doseq [splitter
            (if rat
              [{:suff (str ".trainer." (numerator rat) "." (denominator rat))
                :num (int (* (count the-flows) rat))
                :op take}
               {:suff (str ".tester." (numerator (- 1 rat)) "." (denominator (- 1 rat)))
                :op drop
                :num (int (* (count the-flows) rat))}]
               [{:suff ""
                 :num 0
                 :op NOP2}])]
      (let [out (str flow-filename (splitter :suff)
                     (if tidy-attributes ".tidy" "")
                     ".arff")]
                (let [processed-flows
                      (map (fn [x y] (conj (subvec x (+ proto-index 1)) y)) the-flows labels)]
                  (do
                    ;; Relation section
                    (writeout out "@RELATION application\n\n")
                    ;; Attributes section
                    ;; First, the numerical fields, to which we give boring names:
                    (loop [n 1, fields (first processed-flows)]
                      (writeout out "@ATTRIBUTE f" n "\tNUMERIC\n")
                      (when (seq (rest (rest fields)))
                        (recur (inc n) (rest fields))))
                    ;; Now print the list of services as a set of nominal values
                    (writeout out "@ATTRIBUTE service\t{")
                    (loop [service-list (if tidy-attributes
                                          (distinct labels)
                                          (vals service-map))]
                      (writeout out (first service-list))
                      (when (seq (rest service-list))
                        (writeout out ",")
                        (recur (rest service-list))))
                    (writeout out "}\n\n")
                    ;; Data section
                    (writeout out "@DATA\n")
                    ;; if splitting off testing and training arffs, do it here.
                    (doseq [labelled-flow ((splitter :op) (splitter :num) processed-flows)]
                      (writeout out (apply str (interpose "," labelled-flow)) "\n" ))))
                (println "ARFF file saved as " out)))))

    (defn efs-prepper
      "Formats the flows so that they can be processed by EFS."
      [flowpath rat]
      (let [the-flows (flows flowpath)]
        (doseq [splitter
                (if rat
                  [{:suff (str ".trainer." (numerator rat) "." (denominator rat) ".efs")
                    :num (int (* (count the-flows) rat))
                    :op take}
                   {:suff (str ".tester." (numerator (- 1 rat)) "." (denominator (- 1 rat)) ".efs")
                    :num (int (* (count the-flows) rat))
                    :op drop}]
                  [{:suff ".efs"
                    :num 0
                    :op NOP2}])]      
          (let [out (str flowpath (splitter :suff))
                processed-flows
                ((splitter :op) (splitter :num)
                 (map (fn [x y] (conj x y))
                      (map (fn [x] (subvec x proto-index)) the-flows)
                      (map (fn [x] (min (bigint (x p1))
                                       (bigint (x p2)))) the-flows))) ]
            (doseq [pf processed-flows]
              (writeout out (apply str (interpose "," pf)) "\n" ))
          (println "Saved EFS format to " out)))))
      
    (defn -main
      "Prints a list of labels for the flow provided."
      [& args]
      (let [options (apply hash-map args)]
        (let [flowpath (options "-f")
              tidy (options "-t")
              test-rat (if (options "-r")
                         (read-string (options "-r"))
                         nil)
              output (options "-o")]
          (case output
            "arff"
            (arff-maker flowpath tidy test-rat)
            "efs"
            (efs-prepper flowpath test-rat)
            ;; default
            (println "Usage: java -jar flow-labeller.jar <-f path to flow file> <-o arff|efs> <-r ratio-to-separate-as-training-set> <-t 1|0 (1 to tidy up arff header>")))))













































