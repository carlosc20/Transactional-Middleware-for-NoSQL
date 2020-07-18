package transaction_manager.control;

import certifier.Timestamp;

import java.util.concurrent.CompletableFuture;

public interface CommitControlHandler {
    CompletableFuture<Void> deliver(Timestamp<Long> commitTs);
    void completeDeliveries();
}
