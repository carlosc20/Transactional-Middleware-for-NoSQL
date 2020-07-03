package transaction_manager;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;
import jraft.rpc.TransactionCommitRequest;
import jraft.rpc.TransactionStartRequest;
import jraft.rpc.UpdateTimestampRequest;
import transaction_manager.utils.BitWriteSet;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//TODO change to transaction manager
public class TransactionManagerServer {
    private ManagedMessagingService mms;
    private ExecutorService e;
    private Serializer s;
    private TransactionManager transactionManager;

    public TransactionManagerServer(int port) {
        e = Executors.newFixedThreadPool(1);
        s = new SerializerBuilder()
            .addType(BitWriteSet.class)
            .addType(TransactionCommitRequest.class)
            .addType(TransactionStartRequest.class)
            .addType(UpdateTimestampRequest.class)
            .addType(TransactionImpl.class)
            .build();
        mms = new NettyMessagingService(
            "transaction_manager",
            Address.from(port),
            new MessagingConfig());
        this.transactionManager = new TransactionManagerImpl(0, 0, "mongodb://127.0.0.1:27017", "lei", "teste");
    }

    void start() {
        mms.start();
        mms.registerHandler("start", (a,b) -> {
            System.out.println("start request arrived");
            TransactionStartRequest tsr = s.decode(b);
            return s.encode(transactionManager.startTransaction());
        }, e);

        mms.registerHandler("commit", (a,b) -> {
            System.out.println("commit request arrived");
            TransactionCommitRequest tcr = s.decode(b);
            transactionManager.tryCommit(tcr.getTransactionContentMessage());
            return s.encode(0);
        } ,e);

        mms.registerHandler("get_server_context", (a,b) -> {
            System.out.println("context request arrived");
            return s.encode(transactionManager.getServersContext());
        } ,e);
    }

}
