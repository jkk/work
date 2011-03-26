(defproject work "0.2.8-SNAPSHOT"
  :description "Clojure workers."
  :url "http://github.com/getwoven/work"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [clj-serializer "0.1.1"]
                 [clj-sys/plumbing "0.1.3-SNAPSHOT"]
                 [clj-time "0.2.0-SNAPSHOT"]
                 [store "0.2.2-SNAPSHOT"]
                 [woven/clj-json "0.3.2"]]
  :dev-dependencies [[swank-clojure "1.3.0-SNAPSHOT"]
                     [lein-clojars "0.5.0"]
                     [lein-run "1.0.0"]
                     [robert/hooke "1.1.0"]]
  :test-selectors {:default (fn [v] (not (or (:integration v)
                                             (:system v))))
                   :integration :integration
                   :system :system
                   :independent :independent
                   :all (fn [_] true)}
  :repositories {"snapshots" "http://mvn.getwoven.com/repos/woven-public-snapshots"
                 "releases" "http://mvn.getwoven.com/repos/woven-public-releases"})
