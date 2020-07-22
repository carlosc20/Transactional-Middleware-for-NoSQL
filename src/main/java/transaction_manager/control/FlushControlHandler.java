package transaction_manager.control;

import certifier.Timestamp;

import java.util.concurrent.CompletableFuture;

public interface FlushControlHandler  {
    CompletableFuture<Void> put(Timestamp<Long> commitTimestamp, CompletableFuture<Boolean> timestamp, CompletableFuture<Boolean> writeValues);
    void putPipe(Timestamp<Long> timestamp);
}
