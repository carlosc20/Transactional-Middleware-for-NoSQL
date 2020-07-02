package certifier;

import transaction_manager.BitWriteSet;

public interface Certifier<V> {
    Timestamp<V> start();
    Timestamp<V> commit(BitWriteSet ws, Timestamp<V> ts);
    void update();
    Timestamp<V> getCurrentCommitTs();
    Timestamp<V> getSafeToDeleteTimestamp();
    void evictStoredWriteSets(V newLowWaterMark);
}
