package transaction_manager;

import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;
import jraft.rpc.TransactionCommitRequest;
import jraft.rpc.TransactionStartRequest;
import transaction_manager.messaging.ServerContextRequestMessage;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionContentMessage;

import java.time.Duration;
import java.util.concurrent.*;

public class TransactionManagerStub implements TransactionManager{
    private final ManagedMessagingService mms;
    private final ExecutorService e;
    private final Serializer s;
    private final Address manager;

    public TransactionManagerStub(int myPort, int serverPort){
        e = Executors.newFixedThreadPool(1);
        manager = Address.from(serverPort);
        s = new SerializerBuilder()
                .withRegistrationRequired(false)
                .build();
        mms = new NettyMessagingService(
                "transaction_manager",
                Address.from(myPort),
                new MessagingConfig());
        mms.start();
    }

    @Override
    public Timestamp<Long> startTransaction() {
        TransactionStartRequest tsr = new TransactionStartRequest();
        try {
            return (MonotonicTimestamp) mms.sendAndReceive(manager, "start", s.encode(tsr), Duration.ofSeconds(20), e)
                    .thenApply(s::decode).get();
        } catch (InterruptedException | ExecutionException interruptedException) {
            interruptedException.printStackTrace();
        }
        return null;
    }

    @Override
    public CompletableFuture<Boolean> tryCommit(TransactionContentMessage tx) {
        TransactionCommitRequest tcr = new TransactionCommitRequest(tx);
        return mms.sendAndReceive(manager, "commit", s.encode(tcr), Duration.ofSeconds(20), e)
                .thenApply(s::decode);
    }

    @Override
    public ServersContextMessage getServersContext() {
        try {
            return (ServersContextMessage) mms.sendAndReceive(manager, "get_server_context", s.encode(new ServerContextRequestMessage()), Duration.ofSeconds(20), e)
                    .thenApply(s::decode).get();
        } catch (InterruptedException | ExecutionException interruptedException) {
            interruptedException.printStackTrace();
        }
        return null;
    }
}
