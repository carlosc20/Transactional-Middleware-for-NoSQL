package transaction_manager;

import certifier.Timestamp;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionContentMessage;
import transaction_manager.utils.MessagingService;

import java.util.concurrent.CompletableFuture;

public class TransactionManagerStub implements TransactionManager{

    private MessagingService ms;

    public TransactionManagerStub(int myPort, int serverPort){
        this.ms = new MessagingService(myPort, serverPort);
    }

    @Override
    public Timestamp<Long> startTransaction() {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> tryCommit(TransactionContentMessage tx) {
        return null;
    }

    @Override
    public ServersContextMessage getServersContext() {
        return null;
    }
}
