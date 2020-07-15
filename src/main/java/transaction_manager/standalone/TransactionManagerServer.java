package transaction_manager.standalone;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;
import nosql.KeyValueDriver;
import nosql.MongoAsynchKV;
import npvs.NPVS;
import npvs.NPVSStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionCommitRequest;
import transaction_manager.messaging.TransactionStartRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TransactionManagerServer {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionManagerServer.class);
    private final ManagedMessagingService mms;
    private final ExecutorService e;
    private final Serializer s;
    private final TransactionManagerImpl transactionManagerService;

    //TODO builder pattern
    public TransactionManagerServer(long timestep, int myPort, NPVS<Long> npvs, KeyValueDriver driver, ServersContextMessage scm){
        e = Executors.newFixedThreadPool(1);
        s = new SerializerBuilder()
                .withRegistrationRequired(false)
                .build();

        mms = new NettyMessagingService(
                "transaction_manager",
                Address.from(myPort),
                new MessagingConfig());

        this.transactionManagerService = new TransactionManagerImpl(timestep, npvs, driver, scm);
    }

    public void start() {
        mms.start();
        mms.registerHandler("start", (a,b) -> {
            TransactionStartRequest tsr = s.decode(b);
            LOG.info("New start transaction request message with id: {}", tsr.getId());
            return transactionManagerService.startTransaction().thenApply(s::encode);
        });

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
        long timestep = 1000;
        int npvsStubPort = 30001;
        int npvsPort = 20000;
        String databaseURI = "mongodb://127.0.0.1:27017";
        String databaseName =  "testeLei";
        String databaseCollectionName = "teste1";

        List<Address> npvsServers = new ArrayList<>();
        npvsServers.add(Address.from(20000));
        npvsServers.add(Address.from(20001));

        NPVS<Long> npvs = new NPVSStub(npvsStubPort, npvsServers);
        KeyValueDriver driver = new MongoAsynchKV(databaseURI, databaseName, databaseCollectionName);
        ServersContextMessage scm = new ServersContextMessage(databaseURI, databaseName, databaseCollectionName, npvsServers);
        new TransactionManagerServer(timestep, 30000, npvs, driver, scm).start();
    }
}
