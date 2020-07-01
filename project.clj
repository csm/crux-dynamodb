(defproject com.github.csm/crux-dynamodb "0.1.2-SNAPSHOT"
  :description "Crux TX logs on DynamoDB"
  :url "https://github.com/csm/crux-dynamodb"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [juxt/crux-core "20.06-1.9.1-beta"]
                 [software.amazon.awssdk/dynamodb "2.13.41"]]
  :repl-options {:init-ns crux.dynamodb.repl}
  :java-source-paths ["src"]
  :javac-options ["-target" "8" "-source" "8"])
