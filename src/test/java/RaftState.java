import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import io.atomix.utils.net.Address;
import npvs.NPVSStub;
import npvs.messaging.FlushMessage;
import org.junit.Test;
import transaction_manager.TransactionController;
import transaction_manager.TransactionImpl;
import transaction_manager.TransactionManager;
import transaction_manager.raft.sofa_jraft.RaftTransactionManagerStub;
import transaction_manager.standalone.TransactionManagerStub;
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
    public void dummy() throws InterruptedException{
        ExecutorService e = Executors.newSingleThreadExecutor();
        TransactionManager tms = new TransactionManagerStub(23451, 30000);
        TransactionController transactionController = new TransactionController(Address.from(23415), tms);
        transactionController.buildContext();

        for(int i = 0; i < 100; i++){
            TransactionImpl t1 = null;
            try {
                t1 = transactionController.startTransaction();
            } catch (ExecutionException executionException) {
                executionException.printStackTrace();
            }

            byte[] writeKey1 = "melao".getBytes();
            byte[] writeKey2 = "meloa".getBytes();

            byte[] writeValue1 = "grande".getBytes();
            byte[] writeValue2 = "pequena".getBytes();

            t1.write(writeKey1, writeValue1);
            t1.write(writeKey2, writeValue2);
            t1.commit();
        }
        e.awaitTermination(100000, TimeUnit.SECONDS);
    }

    @Test
    public void dummy2(){
        System.out.println(3001L / 1000L * 1000L + 1000L);
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
