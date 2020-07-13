package transaction_manager;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;
import jraft.rpc.TransactionCommitRequest;
import jraft.rpc.TransactionStartRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StandaloneTMServer {
    private static final Logger LOG = LoggerFactory.getLogger(StandaloneTMServer.class);
    private final ManagedMessagingService mms;
    private final ExecutorService e;
    private final Serializer s;
    private final StandaloneTMService transactionManagerService;

    //TODO builder pattern
    public StandaloneTMServer(int myPort, int npvsStubPort, int npvsPort, String databaseURI, String databaseName, String databaseCollectionName){
        e = Executors.newFixedThreadPool(1);
        s = new SerializerBuilder()
                .withRegistrationRequired(false)
                .build();

        mms = new NettyMessagingService(
                "transaction_manager",
                Address.from(myPort),
                new MessagingConfig());

        this.transactionManagerService = new StandaloneTMService(npvsStubPort, npvsPort, databaseURI, databaseName, databaseCollectionName, 1000);
    }

    void start() {
        mms.start();
        mms.registerHandler("start", (a,b) -> {
            TransactionStartRequest tsr = s.decode(b);
            LOG.info("New start transaction request message with id: {}", tsr.getId());
            return s.encode(transactionManagerService.startTransaction());
        }, e);

        mms.registerHandler("commit", (a,b) -> {
            TransactionCommitRequest tcr = s.decode(b);
            LOG.info("New commit request message with id: {}", tcr.getId());
            return transactionManagerService.tryCommit(tcr.getTransactionContentMessage())
                    .thenApply(s::encode);
        });

        mms.registerHandler("get_server_context", (a,b) -> {
            LOG.info("Context request arrived");
            return s.encode(transactionManagerService.getServersContext());
        } ,e);
    }

    public static void main(String[] args) {
        new StandaloneTMServer(30000, 30001,20000, "mongodb://127.0.0.1:27017", "testeLei", "teste1").start();
    }
}