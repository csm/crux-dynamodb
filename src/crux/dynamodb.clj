(ns crux.dynamodb
  (:require [crux.db :as db]
            [crux.dynamodb.tx :as tx]
            [clojure.spec.alpha :as s]
            [crux.node :as n]
            [clojure.tools.logging :as log])
  (:import (crux.dynamodb DynamoDBConfigurator)
           (software.amazon.awssdk.services.dynamodb DynamoDbAsyncClient)
           (crux.api ICursor)))

(defrecord DynamoDBTxLog [^DynamoDBConfigurator configurator ^DynamoDbAsyncClient client table-name]
  db/TxLog
  (submit-tx [_ tx-events]
    (log/debug "submit-tx events:" (pr-str tx-events))
    (tx/write-events configurator client table-name tx-events))

  (open-tx-log ^ICursor [_ after-tx-id]
    ;(log/debug "open-tx-log" after-tx-id)
    (tx/tx-iterator configurator client table-name after-tx-id))

  (latest-submitted-tx [_]
    (log/debug "latest-submitted-tx")
    {:crux.tx/tx-id @(tx/last-tx client table-name)}))

(s/def ::table-name string?)

(def dynamodb-tx-log
  {::configurator {:start-fn (fn [_ _] (reify DynamoDBConfigurator))}
   ::n/tx-log     {:start-fn (fn [{::keys [configurator]} {::keys [table-name]}]
                               (let [client (.makeClient configurator)]
                                 (log/info "made dynamodb client:" client "checking table" table-name)
                                 @(tx/ensure-table-exists configurator client table-name)
                                 @(tx/ensure-table-ready client table-name)
                                 (log/info "dynamodb table ready, creating tx-log")
                                 (->DynamoDBTxLog configurator client table-name)))
                   :args     {::table-name {:required?        true
                                            :crux.config/type ::table-name
                                            :doc              "DynamoDB Table Name"}}
                   :deps     #{::configurator}}})