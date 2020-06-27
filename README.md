# crux-dynamodb

[![com.github.csm/crux-dynamodb](https://img.shields.io/clojars/v/com.github.csm/crux-dynamodb.svg)](https://clojars.org/com.github.csm/crux-dynamodb)

A [crux](https://opencrux.com) tx-log on AWS DynamoDB.

## Getting Started

You'll need a document store and a KV store. For the document store,
[crux-s3](https://github.com/juxt/crux/tree/master/crux-s3) is a good option.
The KV store can still be in-memory, RocksDB, etc.

```clojure
(crux.api/start-node {:crux.node/topology ['crux.node/base-topology
                                           'crux.dynamodb/dynamodb-tx-log
                                           'crux.s3/s3-doc-store]
                      :crux.dynamodb/table-name "my-dynamodb-table"
                      :crux.dynamodb/configurator {:start-fn (fn [_ _ ] (reify crux.dynamodb.DynamoDBConfigurator))}
                      :crux.s3/bucket "my-s3-bucket"})
```

You can extend the DynamoDBConfigurator interface to provide a custom
DynamoDB client, intercept CreateTable calls for customization, or provide
your own serialization (uses Nippy by default).

## Details

The key schema for DynamoDB is:

* `part`, type `N`, key type `HASH`
* `tx`, type `N`, key type `RANGE`

A 64-bit transaction ID in crux is decomposed into the following:

```
┌────┬───────┬──────┐
│ 64 │ 15-63 │ 1-14 │
│  0 │  part │   tx │
└────┴───────┴──────┘
```

So there are up to 16k transactions per partition, and 562949953421311 possible partitions (so you won't run out). 

The `part` key in DynamoDB is exactly the same per tx-log entry;
the `tx` key is the `tx` value shifted left 17 bits, and the bottom 17
bits are for each individual transaction event. This means you can have
up to 131,071 events per transaction.

The special partition `0x2000000000000` is for tx-log metadata, and the only
value is the "current tx ID" stored in tx 0. This value in DynamoDB contains
values `currentPartition` and `currentTx`.

When writing a transaction, we first read `{part=0x2000000000000,tx=0}`, and
increment `currentTx` by one. If it overflows, increment `currentPartition` by
one and use a tx value of 0. We update this key conditionally, so multiple writers
can write concurrently, just retrying if the conditional update fails.

## License

Copyright © 2020 FIXME

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
