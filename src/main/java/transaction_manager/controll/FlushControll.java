package transaction_manager.controll;

import certifier.Timestamp;

import java.util.concurrent.CompletableFuture;

public interface FlushControll{

    CompletableFuture<Void> deliver(Timestamp<Long> commitTs);
    void completeDeliveries();
}
