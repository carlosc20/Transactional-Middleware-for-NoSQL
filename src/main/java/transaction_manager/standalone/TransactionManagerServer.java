package transaction_manager.standalone;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;
import nosql.KeyValueDriver;
import nosql.MongoAsynchKV;
import npvs.NPVSStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionCommitRequest;
import transaction_manager.messaging.TransactionStartRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TransactionManagerServer {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionManagerServer.class);
    private final ManagedMessagingService mms;
    private final ExecutorService e;
    private final Serializer s;
    private final TransactionManagerImpl transactionManagerService;
    private final NPVSStub npvs;


    public TransactionManagerServer(int batchTimeout, long timestep, int myPort, NPVSStub npvs, KeyValueDriver driver, ServersContextMessage scm){
        e = Executors.newFixedThreadPool(1);
        s = new SerializerBuilder()
                .withRegistrationRequired(false)
                .build();

        mms = new NettyMessagingService(
                "transaction_manager",
                Address.from(myPort),
                new MessagingConfig());

        this.npvs = npvs;
        this.transactionManagerService = new TransactionManagerImpl(batchTimeout ,timestep, npvs, driver, scm);
    }

    public void start() {
        mms.start();
        mms.registerHandler("start", (a,b) -> {
            TransactionStartRequest tsr = s.decode(b);
            return transactionManagerService.startTransaction().thenApply(s::encode);
        });

        mms.registerHandler("commit", (a,b) -> {
            TransactionCommitRequest tcr = s.decode(b);
            return transactionManagerService.tryCommit(tcr.getTransactionContentMessage())
                    .thenApply(s::encode);
        });

        mms.registerHandler("get_server_context", (a,b) -> {
            LOG.info("Context request arrived");
            return s.encode(transactionManagerService.getServersContext());
        } ,e);

        //warmhup
        List<String> handlers = new ArrayList<>();
        handlers.add("put");
        handlers.add("evict");
        try {
            this.npvs.warmhup(handlers).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        int batchTimeout = 200;
        long timestep = 1000;
        Address npvsStubPort = Address.from(30001);
        String databaseURI = "mongodb://127.0.0.1:27017";
        String databaseName =  "testeLei";
        String databaseCollectionName = "teste1";

        ArrayList<String> npvsServers = new ArrayList<>();
        npvsServers.add("localhost:20000");
        npvsServers.add("localhost:20001");

        NPVSStub npvs = new NPVSStub(npvsStubPort, npvsServers);
        KeyValueDriver driver = new MongoAsynchKV(databaseURI, databaseName, databaseCollectionName);
        ServersContextMessage scm = new ServersContextMessage(databaseURI, databaseName, databaseCollectionName, npvsServers);
        new TransactionManagerServer(batchTimeout,timestep, 30000, npvs, driver, scm).start();
    }
}
