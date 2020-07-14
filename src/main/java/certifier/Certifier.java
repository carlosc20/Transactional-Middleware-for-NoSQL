package certifier;

import transaction_manager.utils.BitWriteSet;

import java.util.concurrent.CompletableFuture;

public interface Certifier<V> {
    CompletableFuture<Timestamp<V>> start();
    Timestamp<V> commit(BitWriteSet ws, Timestamp<V> ts);
    void update(Timestamp<Long> commitTimestamp);
    Timestamp<V> getCurrentCommitTs();
    Timestamp<V> getSafeToDeleteTimestamp();
    void evictStoredWriteSets(V newLowWaterMark);
}
