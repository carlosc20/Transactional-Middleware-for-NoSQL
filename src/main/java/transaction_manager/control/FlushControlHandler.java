package transaction_manager.control;

import java.util.concurrent.CompletableFuture;

public interface FlushControlHandler {
    CompletableFuture<Void> put(CompletableFuture<Void> timestamp, CompletableFuture<Void> writeValues);
}
