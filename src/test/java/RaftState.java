import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import io.atomix.utils.net.Address;
import org.junit.Test;
import transaction_manager.TransactionController;
import transaction_manager.TransactionImpl;
import transaction_manager.raft.sofa_jraft.RaftTransactionManagerStub;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class RaftState {
    //RaftTransactionManagerStub tms = new RaftTransactionManagerStub( "manager", "127.0.0.1:8081,127.0.0.1:8082");
    RaftTransactionManagerStub tms = null;
    @Test
    public void readState(){
        System.out.println(tms.getExtendedState(1).toString());
    }

    @Test
    public void dummy(){
        LocalDateTime l1 = LocalDateTime.now();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(Duration.between(l1, LocalDateTime.now()).getSeconds());
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
