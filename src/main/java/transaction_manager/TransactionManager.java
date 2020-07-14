package transaction_manager;

import certifier.Timestamp;

import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionContentMessage;

import java.util.concurrent.CompletableFuture;

public interface TransactionManager{
    CompletableFuture<Timestamp<Long>> startTransaction();
    CompletableFuture<Boolean> tryCommit(TransactionContentMessage tx);
    ServersContextMessage getServersContext();

}
