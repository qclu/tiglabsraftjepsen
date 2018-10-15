(ns jepsen.raftstore
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.data.json :as json]
            [clj-http.client :as cljclient]
            [clojure.tools.logging :as log]
            [slingshot.slingshot :refer [try+]]
            [knossos.model :as model]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [independent :as independent]
                    [nemesis :as nemesis]
                    [tests :as tests]
                    [util :as util :refer [timeout]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(def dir     "/export/raftstore")
(def binary  "raftstore")
(def logfile "/export/raftstore/raftstore.log")
(def pidfile "/export/raftstore/raftstore.pid")
(def config-file "/export/raftstore/raftsrv.toml")
(def log-dir "/export/raftstore/log")
(def data-dir "/export/raftstore/data")
(def service-port "8817")

(def obj_set "/obj/create")
(def obj_get "/obj/get")
(def obj_del "/obj/delete")

(defn str->long [number-string]
    ;(try (Long/parseLong number-string)    
    ;(info (type number-string))
    ;(info number-string)
    (if-not (nil? number-string)
    (try (-> number-string Long/valueOf)    
         (catch Exception e (log/error (str "parse str [" number-string "] to int failed," "exception: " (.getMessage e)))))))

(defn obj-set! 
  [node objId objVal] 
  (def res (->> (str node obj_set)
                (#(cljclient/post % {:query-params {:obj_id (str objId)
                                                   :obj_val (str objVal)}}))))
  (def jsonres (json/read-str (get res :body))) 
  (vector (str->long objId) (str->long (get jsonres "data"))))

(defn obj-get! 
  [node objId] 
  (def res (->> (str node obj_get)
                (#(cljclient/get % {:query-params {:obj_id (str objId)}}))))
  (if ( = (get res :status) 200)
  (str "status: " (get res :status)))
  ;(info "data " (get res :body))
  (def jsonres (json/read-str (get res :body)))
  ;(info "data to parse: " (get jsonres "data"))
  (str->long (get jsonres "data")))

(defn node-url
  "An HTTP url for connecting to a node on a particular port."
  [node]
  (str "http://" (name node) ":" service-port))

(defn peer-url
  "The HTTP url for other peers to talk to a node."
  [node]
  (node-url node 2380))

(defn client-url
  "The HTTP url clients use to talk to a node."
  [node]
  (node-url node 2379))

(defn initial-cluster
  "Constructs an initial cluster string for a test, like
  \"foo=foo:2380,bar=bar:2380,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (str (name node) "=" (peer-url node))))
       (str/join ",")))

(defn db
  "RaftStore DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (info node "installing raftstore" version)
        ; Remove log file first.
        (c/exec :mkdir log-dir)
        (c/exec :mkdir data-dir)
        (cu/start-daemon!
          {:logfile logfile
           :pidfile pidfile
           :chdir   dir}
          binary
          :-c config-file)

        (Thread/sleep 8000)))

    (teardown! [_ test node]
      (info node "tearing down raftstore")
      (cu/stop-daemon! binary pidfile)
      (c/su
        (c/exec :rm :-rf data-dir)               
        (c/exec :rm :-rf log-dir)))               

    db/LogFiles
    (log-files [_ test node]
      [logfile])))

(defn parse-long
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (Long/parseLong s)))

(defn client
  "A client for a single compare-and-set register"
  [conn]
  (reify client/Client
    (open! [_ test node]
      (client (node-url node)))
      
    (invoke! [this test op]
      (let [[k v] (:value op)
            crash (if (= :read (:f op)) :fail :info)]
        ;(info "key: " k "val:" v "conn: " conn)
        (try+
         (case (:f op)
           :read (let [value (->
                               (obj-get! conn k))]
                   (assoc op :type :ok, :value (independent/tuple k value)))

           :write (do (obj-set! conn k v)
                      (assoc op :type, :ok)))

         (catch java.net.UnknownHostException e
           (assoc op :type crash, :error e))

         (catch java.net.SocketTimeoutException e
           (assoc op :type crash, :error :timeout))

         (catch [:errorCode 100] e
           (assoc op :type :fail, :error :not-found))

         (catch [:body "command failed to be committed due to node failure\n"] e
           (assoc op :type crash :error :node-failure))

;         (catch [:status 502] e
;           (assoc op :type crash :error e))

         (catch [:status 307] e
           (assoc op :type crash :error :redirect-loop))

         (catch (and (instance? clojure.lang.ExceptionInfo %)) e
           (assoc op :type crash :error e))

         (catch (and (:errorCode %) (:message %)) e
           (assoc op :type crash :error e)))))

    ; If our connection were stateful, we'd close it here.
    ; Verschlimmbesserung doesn't hold a connection open, so we don't need to.
    (close! [_ _])

    (setup! [_ _])
    (teardown! [_ _])))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})

(defn raftstore-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (info :opts opts)
  (merge tests/noop-test
         {:name "raftstore"
          :os debian/os
          :db (db "v3.1.5")
          :client (client nil)
          ;:nemesis (nemesis/partition-random-halves)
          :model  (model/cas-register)
          :checker (checker/compose
                     {:perf     (checker/perf)
                      :indep (independent/checker
                               (checker/compose
                                 {:timeline (timeline/html)
                                  :linear   (checker/linearizable)}))})
          :generator (->> (independent/concurrent-generator
                            10
                            (range)
                            (fn [k]
                              (->> (gen/mix [r w])
                                   (gen/stagger 1/30)
                                   (gen/limit 300))))
                          (gen/nemesis nil)
                          ;  (gen/seq (cycle [(gen/sleep 5)
                          ;                   {:type :info, :f :start}
                          ;                   (gen/sleep 5)
                          ;                   {:type :info, :f :stop}])))
                          (gen/time-limit (:time-limit opts)))}
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn raftstore-test})
                   (cli/serve-cmd))
            args))
