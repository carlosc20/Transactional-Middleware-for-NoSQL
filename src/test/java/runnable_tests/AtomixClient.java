package runnable_tests;

import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;
import transaction_manager.messaging.*;
import utils.timer.Timer;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AtomixClient {

    private final ManagedMessagingService mms;
    private final ExecutorService e;
    private final Serializer s;
    private final Address manager;
    private CompletableFuture<Object> res;

    public AtomixClient(int myPort, int serverPort){
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

        this.mms.registerHandler("reply", (a,b) -> {
            try{
                res.complete(s.decode(b));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, e);
    }

    public CompletableFuture<Object> sendAndReceive() {
        //TransactionCommitRequest tcr = new TransactionCommitRequest();
        TransactionStartRequest tsr = new TransactionStartRequest();
        return mms.sendAndReceive(manager, "normal", s.encode(1), Duration.ofSeconds(20), e)
                .thenApply(s::decode);
    }

    public CompletableFuture<Object> sendAndReceiveSplit() throws ExecutionException, InterruptedException {
        res = new CompletableFuture<>();
        mms.sendAsync(manager, "separado", s.encode(1));
        return res;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Timer timer = new Timer();
        timer.addCheckpoint("Start");
        AtomixClient tms = new AtomixClient( 12346, 30000);
        tms.sendAndReceive().get();
        timer.addCheckpoint("End");
        timer.print();
    }
}
