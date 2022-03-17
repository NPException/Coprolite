(defproject coprolite "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.11.0-rc1"]
                 [criterium "0.4.6"]]
  :main ^:skip-aot de.npcomplete.coprolite.core
  :target-path "target/%s"
  :global-vars {*warn-on-reflection* true}
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
