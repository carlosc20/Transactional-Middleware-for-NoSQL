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
import transaction_manager.standalone.TransactionManagerImpl;
import transaction_manager.standalone.TransactionManagerServer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AtomixHandler {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionManagerServer.class);
    private final ManagedMessagingService mms;
    private final ExecutorService e;
    private final Serializer s;

    public AtomixHandler(int myPort){
        e = Executors.newFixedThreadPool(1);
        s = new SerializerBuilder()
                .withRegistrationRequired(false)
                .build();

        mms = new NettyMessagingService(
                "transaction_manager",
                Address.from(myPort),
                new MessagingConfig());
    }

    public void start() {
        mms.start();
        mms.registerHandler("normal", (a,b) -> {
            return s.encode(b);
        } ,e);

        mms.registerHandler("separado", (a,b) -> {
            mms.sendAsync(a, "reply", s.encode(b));
        }, e);
    }

    public static void main(String[] args) {
        new AtomixHandler(30000).start();
    }
}
