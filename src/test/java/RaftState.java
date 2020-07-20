import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import io.atomix.utils.net.Address;
import npvs.NPVSStub;
import npvs.messaging.FlushMessage;
import org.junit.Test;
import transaction_manager.TransactionController;
import transaction_manager.TransactionImpl;
import transaction_manager.raft.sofa_jraft.RaftTransactionManagerStub;
import utils.WriteMapsBuilder;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RaftState {
    //RaftTransactionManagerStub tms = new RaftTransactionManagerStub( "manager", "127.0.0.1:8081,127.0.0.1:8082");
    RaftTransactionManagerStub tms = null;
    @Test
    public void readState(){
        System.out.println(tms.getExtendedState(1).toString());
    }

    @Test
    public void dummy() throws InterruptedException, ExecutionException {
        ExecutorService e = Executors.newSingleThreadExecutor();
        ArrayList<String> servers = new ArrayList<>();
        servers.add("localhost:" + 20000);
        servers.add("localhost:" + 20001);
        NPVSStub npvs = new NPVSStub(Address.from(23551), servers);
        WriteMapsBuilder wmb = new WriteMapsBuilder();
        wmb.put(1, "marco", "dantas");
        wmb.put(1, "sadfa", "dantas");
        wmb.put(1, "rco", "dantas");


        ArrayList<String> handlers = new ArrayList<>();
        handlers.add("put");
        handlers.add("get");
        handlers.add("eviction");
        npvs.warmhup(handlers).get();
        System.out.println("Warmup done. Sleeping 1 sec");
        Thread.sleep(1000);

        for (int i = 0; i < 100; i++){
            final int r = i;
            npvs.put(new FlushMessage(wmb.getWriteMap(1), new MonotonicTimestamp(r), new MonotonicTimestamp(r)))
                    .thenAccept(x -> System.out.println("request " + r));}

        e.awaitTermination(100000, TimeUnit.SECONDS);
    }

    @Test
    public void write() throws ExecutionException, InterruptedException {
        TransactionController transactionController = new TransactionController(Address.from(23415), tms);
        transactionController.buildContext();
        TransactionImpl t = transactionController.startTransaction();
        byte[] writeKey1 = "melao1".getBytes();
        byte[] writeKey2 = "meloa1".getBytes();

        byte[] writeValue1 = "grande".getBytes();
        byte[] writeValue2 = "pequena".getBytes();

        t.write(writeKey1, writeValue1);
        t.write(writeKey2, writeValue2);
        t.commit();
    }
}
