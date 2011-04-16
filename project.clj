(defproject work "1.0.0-SNAPSHOT"
  :description "Clojure workers."
  :url "http://github.com/getwoven/work"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [clj-sys/plumbing "0.1.4-SNAPSHOT"]
                 [store "0.2.2-SNAPSHOT"]]
  :dev-dependencies [[swank-clojure "1.3.0-SNAPSHOT"]]
  :repositories {"snapshots" "http://mvn.getwoven.com/repos/woven-public-snapshots"
                 "releases" "http://mvn.getwoven.com/repos/woven-public-releases"})
