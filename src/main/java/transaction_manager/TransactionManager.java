package transaction_manager;

import transaction_manager.messaging.TransactionContentMessage;

import java.util.concurrent.CompletableFuture;

public interface TransactionManager<V> {

    Transaction startTransaction();
    CompletableFuture<Boolean> tryCommit(TransactionContentMessage<V> tx);
}
