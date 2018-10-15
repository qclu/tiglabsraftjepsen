(defproject jepsen.raftstore "0.5.10-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.raftstore
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/data.json "0.2.6"]
                 [jepsen "0.1.10"]
                 [org.clojure/tools.logging "0.2.6"]
                 [clj-http "3.9.1"]
                 [cheshire "5.4.0"]])
