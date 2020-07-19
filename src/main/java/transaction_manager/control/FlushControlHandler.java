package transaction_manager.control;

import java.util.concurrent.CompletableFuture;

public interface FlushControlHandler {
    CompletableFuture<Void> put(CompletableFuture<Boolean> timestamp, CompletableFuture<Boolean> writeValues);
}
