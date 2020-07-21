package certifier;

import transaction_manager.utils.BitWriteSet;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Certifier<V> extends Serializable {
    CompletableFuture<Timestamp<V>> start();
    Timestamp<V> commit(BitWriteSet ws, Timestamp<V> ts);
    void update(Timestamp<Long> commitTimestamp);
    Timestamp<V> getCurrentCommitTs();
    Timestamp<V> getSafeToDeleteTimestamp();
    void evictStoredWriteSets(V newLowWaterMark);
    Timestamp<Long> getForceDeleteTimestamp(LocalDateTime eventTime, long intervalSec);
    void setTombstone(LocalDateTime value);
    void transactionEnded(Timestamp<Long> startTimestamp);
    void transactionStarted(Timestamp<Long> startTimestamp);
}
