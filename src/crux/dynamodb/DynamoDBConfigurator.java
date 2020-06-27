package crux.dynamodb;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;

public interface DynamoDBConfigurator {
    default CreateTableRequest.Builder createTable(CreateTableRequest.Builder builder) { return builder; }

    default DynamoDbAsyncClient makeClient() { return DynamoDbAsyncClient.create(); }

    default byte[] freeze(Object tx) { return NippySerde.freeze(tx); }

    default Object thaw(byte[] bytes) { return NippySerde.thaw(bytes); }
}

class NippySerde {
    private static final IFn requiringResolve = Clojure.var("clojure.core/requiring-resolve");
    private static final IFn fastFreeze = (IFn) requiringResolve.invoke(Clojure.read("taoensso.nippy/fast-freeze"));
    private static final IFn fastThaw = (IFn) requiringResolve.invoke(Clojure.read("taoensso.nippy/fast-thaw"));

    static byte[] freeze(Object doc) {
        return (byte[]) fastFreeze.invoke(doc);
    }

    static Object thaw(byte[] bytes) {
        return fastThaw.invoke(bytes);
    }
}
