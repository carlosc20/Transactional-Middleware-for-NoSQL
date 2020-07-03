package npvs;

import certifier.Timestamp;
import utils.ByteArrayWrapper;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface NPVS<V> {

    CompletableFuture<Void> put(Map<ByteArrayWrapper,byte[]> writeMap, Timestamp<V> ts);
    CompletableFuture<byte[]> get(ByteArrayWrapper key, Timestamp<V> ts);
}
