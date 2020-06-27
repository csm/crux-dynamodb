(ns crux.dynamodb.tx
  (:require [crux.io :as cio]
            [clojure.tools.logging :as log])
  (:import (software.amazon.awssdk.services.dynamodb DynamoDbAsyncClient)
           (software.amazon.awssdk.services.dynamodb.model GetItemRequest AttributeValue GetItemResponse UpdateItemRequest PutItemRequest CreateTableRequest AttributeDefinition ScalarAttributeType KeySchemaElement KeyType ProvisionedThroughput ConditionalCheckFailedException BatchWriteItemRequest PutRequest ScanRequest ScanResponse DescribeTableRequest DescribeTableResponse ResourceNotFoundException WriteRequest TableStatus)
           (java.util.function Function)
           (java.util.concurrent Executors ScheduledExecutorService TimeUnit CompletableFuture ExecutionException)
           (crux.dynamodb DynamoDBConfigurator)
           (software.amazon.awssdk.core SdkBytes)
           (java.util Date Collection)))

; The maximum number of tx-ids per partition
(def +max-tx-id+ 0x3fff)
(def +max-per-tx+ 0x1ffff)
(def +max-part-id+ 0x1ffffffffffff)
(def +db-part-id+ (inc +max-part-id+))

(defn id->tx-id
  [part tx]
  (bit-or (bit-shift-left part 14)
          (bit-shift-right tx 17)))

(defn tx-id->id
  "Gives the starting [part tx] for a given tx-id. E.g. the dynamo key with
  event 0 in that transaction ID."
  [tx-id]
  [(bit-and +max-part-id+ (bit-shift-right tx-id 14))
   (bit-shift-left (bit-and tx-id +max-tx-id+) 17)])

(defn item-id
  [tx i]
  (bit-or (bit-shift-left tx 17) i))

(defn tx-id->end-id
  [tx-id]
  [(bit-and +max-part-id+ (bit-shift-right tx-id 14))
   (bit-or (bit-shift-left (bit-and tx-id +max-tx-id+) 17) +max-per-tx+)])

(definline ->part
  [txid]
  `(bit-and +max-part-id+ (bit-shift-right ~txid 14)))

(definline ->tx
  [txid]
  `(bit-and ~txid +max-tx-id+))

(def ^:private retry-executor (delay (Executors/newSingleThreadScheduledExecutor)))

(defn- B
  [b]
  (-> (AttributeValue/builder) (.b (SdkBytes/fromByteArray b)) (.build)))

(defn- N
  [n]
  (-> (AttributeValue/builder) (.n (str n)) (.build)))

(def ^:private -catch (reify Function (apply [_ e] e)))

(def ^:private -nop (reify Callable (call [_] nil)))

(defn- ^CompletableFuture delayed-future
  [delay ^TimeUnit delay-units]
  (let [future (CompletableFuture.)]
    (.schedule retry-executor (reify Callable
                                (call [_] (.complete future true)))
               (long delay) delay-units)
    future))

(def ^:private attribute-definitions [(-> (AttributeDefinition/builder)
                                          (.attributeName "part")
                                          (.attributeType ScalarAttributeType/N)
                                          (.build))
                                      (-> (AttributeDefinition/builder)
                                          (.attributeName "tx")
                                          (.attributeType ScalarAttributeType/N)
                                          (.build))])

(def ^:private key-schema [(-> (KeySchemaElement/builder)
                               (.attributeName "part")
                               (.keyType KeyType/HASH)
                               (.build))
                           (-> (KeySchemaElement/builder)
                               (.attributeName "tx")
                               (.keyType KeyType/RANGE)
                               (.build))])

(defn create-table
  [^DynamoDBConfigurator configurator ^DynamoDbAsyncClient client table-name]
  (let [request (-> (CreateTableRequest/builder)
                    (.tableName table-name)
                    (.attributeDefinitions ^Collection attribute-definitions)
                    (.keySchema ^Collection key-schema)
                    (.provisionedThroughput ^ProvisionedThroughput (-> (ProvisionedThroughput/builder)
                                                                       (.readCapacityUnits 1)
                                                                       (.writeCapacityUnits 1)
                                                                       (.build)))
                    (->> (.createTable configurator))
                    (.build))]
    (.createTable client ^CreateTableRequest request)))

(defn ensure-table-exists
  [^DynamoDBConfigurator configurator ^DynamoDbAsyncClient client table-name]
  (-> (.describeTable client ^DescribeTableRequest (-> (DescribeTableRequest/builder)
                                                       (.tableName table-name)
                                                       (.build)))
      (.exceptionally
        (reify Function
          (apply [_ e]
            (when-not (or (instance? ResourceNotFoundException e)
                          (instance? ResourceNotFoundException (.getCause e))) nil
              (throw e)))))
      (.thenCompose
        (reify Function
          (apply [_ response]
            (if response
              (if (or (not= attribute-definitions (-> ^DescribeTableResponse response (.table) (.attributeDefinitions)))
                      (not= key-schema (-> ^DescribeTableResponse response (.table) (.keySchema))))
                (throw (ex-info "table exists but has an invalid schema"
                                {:attribute-definitions (-> ^DescribeTableResponse response (.table) (.attributeDefinitions))
                                 :key-schema (-> ^DescribeTableResponse response (.table) (.keySchema))
                                 :required-attribute-definitions attribute-definitions
                                 :required-key-schema key-schema}))
                (CompletableFuture/completedFuture :ok))
              (create-table configurator client table-name)))))))

(defn ensure-table-ready
  [^DynamoDbAsyncClient client table-name]
  (-> (.describeTable client ^DescribeTableRequest (-> (DescribeTableRequest/builder)
                                                       (.tableName table-name)
                                                       (.build)))
      (.thenCompose
        (reify Function
          (apply [_ result]
            (if (some-> ^DescribeTableResponse result (.table) (.tableStatus) (= TableStatus/ACTIVE))
              (CompletableFuture/completedFuture true)
              (.thenCompose (delayed-future 1 TimeUnit/SECONDS)
                            (reify Function
                              (apply [_ _]
                                (ensure-table-ready client table-name))))))))))

; tx IDs encoded as:
; [1 bit 0] [49 bits partition ID] [14 bits tx ID]
;
; In dynamodb, our partition key 'part' is the partition ID right-shifted 14 bits
; our range key 'tx' is the tx ID, left-shifted 17 bits, or'd with the element ID

(defn- init-tx-info
  [^DynamoDbAsyncClient client table-name]
  (log/info (pr-str {:task ::init-tx-info :phase :begin}))
  (let [start (System/currentTimeMillis)
        part 0
        tx 0
        request (-> (PutItemRequest/builder)
                    (.tableName table-name)
                    (.item {"part"             (N +db-part-id+)
                            "tx"               (N 0)
                            "currentPartition" (N part)
                            "currentTx"        (N tx)})
                    (.conditionExpression "attribute_not_exists(#part) AND attribute_not_exists(#tx)")
                    (.expressionAttributeNames {"#part" "part"
                                                "#tx" "tx"})
                    (.build))]
    (-> (.putItem client ^PutItemRequest request)
        (.thenApply (reify Function
                      (apply [_ _]
                        (log/info (pr-str {:task ::init-tx-info :phase :end :ms (- (System/currentTimeMillis) start)}))
                        [part tx]))))))

(defn- increment-tx
  [^DynamoDbAsyncClient client table-name part tx]
  (log/info (pr-str {:task ::increment-tx :phase :begin :part part :tx tx}))
  (let [start (System/currentTimeMillis)
        [next-part next-tx] (if (< tx +max-tx-id+)
                              [part (inc tx)]
                              [(inc part) 0])
        request (-> (UpdateItemRequest/builder)
                    (.tableName table-name)
                    (.updateExpression "SET #part = :newPart, #tx = :newTx")
                    (.conditionExpression "#part = :oldPart AND #tx = :oldTx")
                    (.key {"part" (N +db-part-id+)
                           "tx"   (N 0)})
                    (.expressionAttributeNames {"#part" "currentPartition"
                                                "#tx"   "currentTx"})
                    (.expressionAttributeValues {":newPart" (N next-part)
                                                 ":newTx"   (N next-tx)
                                                 ":oldPart" (N part)
                                                 ":oldTx"   (N tx)})
                    (.build))]
    (-> (.updateItem client ^UpdateItemRequest request)
        (.thenApply (reify Function
                      (apply [_ _]
                        (log/info (pr-str {:task ::increment-tx :phase :end :next-part next-part :next-tx next-tx :ms (- (System/currentTimeMillis) start)}))
                        [next-part next-tx]))))))

(defn select-next-txid
  [^DynamoDbAsyncClient client table-name & {:keys [delay] :or {delay 10}}]
  (let [start (System/currentTimeMillis)]
    (log/info (pr-str {:task ::select-next-txid :phase :begin}))
    (let [get-request (-> (GetItemRequest/builder)
                          (.tableName table-name)
                          (.consistentRead true)
                          (.attributesToGet ["currentPartition" "currentTx"])
                          (.key {"part" (N +db-part-id+)
                                 "tx"   (N 0)})
                          (.build))]
      (-> (.getItem client ^GetItemRequest get-request)
          (.thenCompose (reify Function
                          (apply [_ response]
                            (log/debug (pr-str {:task ::select-next-txid :phase :read-db-info :info response :ms (- (System/currentTimeMillis) start)}))
                            (if (.hasItem ^GetItemResponse response)
                              (increment-tx client table-name (-> ^GetItemResponse response (.item) ^AttributeValue (get "currentPartition") (.n) (Long/parseLong))
                                            (-> ^GetItemResponse response (.item) ^AttributeValue (get "currentTx") (.n) (Long/parseLong)))
                              (init-tx-info client table-name)))))
          (.exceptionally -catch)
          (.thenCompose
            (reify Function
              (apply [_ result]
                (if (instance? Throwable result)
                  (if (or (instance? ConditionalCheckFailedException result)
                          (instance? ConditionalCheckFailedException (.getCause ^Throwable result)))
                    (-> (delayed-future delay TimeUnit/MILLISECONDS)
                        (.thenCompose (reify Function
                                        (apply [_ _]
                                          (select-next-txid client table-name :delay (max 60000 (long (* delay 1.5))))))))
                    (throw result))
                  (do
                    (log/info (pr-str {:task ::select-next-txid :phase :end :result result :ms (- (System/currentTimeMillis) start)}))
                    (CompletableFuture/completedFuture result))))))))))

(defn- write-batched
  [^DynamoDbAsyncClient client table-name batches]
  (if-let [batch (first batches)]
    (let [request (-> (BatchWriteItemRequest/builder)
                      (.requestItems {table-name batch})
                      (.build))]
      (log/debug "writing batched items:" request)
      (-> (.batchWriteItem client ^BatchWriteItemRequest request)
          (.thenCompose
            (reify Function
              (apply [_ _]
                (write-batched client table-name (rest batches)))))))
    (CompletableFuture/completedFuture nil)))

(defn write-events
  [^DynamoDBConfigurator configurator ^DynamoDbAsyncClient client table-name tx-events]
  (log/info (pr-str {:task ::write-events :phase :begin}))
  (let [start (System/currentTimeMillis)]
    (if (>= (count tx-events) +max-per-tx+)
      (throw (IllegalArgumentException. (format "too many events in transaction, max %d have %d" (dec +max-per-tx+) (count tx-events))))
      (-> (select-next-txid client table-name)
          (.thenCompose
            (reify Function
              (apply [_ [part tx]]
                (let [tx-date (Date.)
                      batches (->> (cons [::meta tx-date] tx-events)
                                   (map-indexed
                                     (fn [i event]
                                       (-> (WriteRequest/builder)
                                           (.putRequest (-> (PutRequest/builder)
                                                            (.item {"part" (N part)
                                                                    "tx"   (N (item-id tx i))
                                                                    "data" (B (.freeze configurator event))})
                                                            (.build)))
                                           (.build))))
                                   (partition-all 25))]
                  (-> (write-batched client table-name batches)
                      (.thenApply (reify Function
                                    (apply [_ _]
                                      (log/info (pr-str {:task ::write-events :phase :end :ms (- (System/currentTimeMillis) start)}))
                                      {:crux.tx/tx-id   (id->tx-id part (bit-shift-left tx 17))
                                       :crux.tx/tx-time tx-date}))))))))))))

(declare last-tx)

(defn- scan-seq
  [part ^DynamoDbAsyncClient client ^ScanRequest request]
  (lazy-seq
    ; (log/debug "scan-seq" request)
    (let [response ^ScanResponse @(.scan client request)]
      ; (log/debug "scan response: " response)
      (if (and (pos? (.count response))
               (not-empty (.lastEvaluatedKey response)))
        (cons (.items response)
              (scan-seq part client
                        (-> request
                            (.toBuilder)
                            (.exclusiveStartKey (.lastEvaluatedKey response))
                            (.build))))
        (let [next-part (inc part)
              max-part (some-> (last-tx client (.tableName request))
                               (deref)
                               (:crux.tx/tx-id)
                               (->part))]
          ;(log/debug "scan-seq next-part" next-part "max-part" max-part)
          (if (and (some? max-part) (<= next-part max-part))
            (cons (.items response)
                  (scan-seq next-part client
                            (-> request
                                (.toBuilder)
                                (.exclusiveStartKey nil)
                                (.expressionAttributeValues {":part" (N next-part)}))))
            [(.items response)]))))))

(defn- item->txid
  [item]
  (when-let [part (some-> item ^AttributeValue (get "part") (.n) (Long/parseLong))]
    (when-let [tx (-> item ^AttributeValue (get "tx") (.n) (Long/parseLong))]
      (id->tx-id part tx))))

(defn- group->tx-info
  [^DynamoDBConfigurator configurator group]
  (log/debug "group->tx-info group: " (pr-str group))
  (let [[meta & events] group
        txid (item->txid meta)
        meta (.thaw configurator (-> meta ^AttributeValue (get "data") (.b) (.asByteArray)))]
    {:crux.tx/tx-id txid
     :crux.tx/tx-time (second meta)
     :crux.tx.event/tx-events (map #(.thaw configurator (.asByteArray (.b ^AttributeValue (get % "data")))) events)}))

(defn tx-iterator
  [^DynamoDBConfigurator configurator ^DynamoDbAsyncClient client table-name after-tx-id]
  (let [start-tx-id (when after-tx-id (tx-id->end-id after-tx-id))
        scan-request (.build (cond-> (-> (ScanRequest/builder)
                                         (.tableName table-name)
                                         (.filterExpression "#part = :part")
                                         (.expressionAttributeNames {"#part" "part"})
                                         (.expressionAttributeValues {":part" (N (or (first start-tx-id) 0))}))
                                     (some? start-tx-id) (.exclusiveStartKey {"part" (N (first start-tx-id))
                                                                              "tx"   (N (second start-tx-id))})))
        tx-iter (sequence (comp (mapcat identity)
                                (partition-by item->txid)
                                (map (partial group->tx-info configurator)))
                          (scan-seq (or (first start-tx-id) 0) client scan-request))]
    (cio/->cursor #() tx-iter)))

(defn last-tx
  [^DynamoDbAsyncClient client table-name]
  (-> (.getItem client ^GetItemRequest (-> (GetItemRequest/builder)
                                           (.tableName table-name)
                                           (.key {"part" (N +db-part-id+)
                                                  "tx"   (N 0)})
                                           (.build)))
      (.thenApply
        (reify Function
          (apply [_ item]
            (when item
              (when-let [item (.item ^GetItemResponse item)]
                (when-let [part (some-> item ^AttributeValue (get "currentPartition") (.n) (Long/parseLong))]
                  (when-let [tx (some-> item ^AttributeValue (get "currentTx") (.n) (Long/parseLong))]
                    {:crux.tx/tx-id (id->tx-id part tx)})))))))))