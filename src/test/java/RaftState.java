import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import io.atomix.utils.net.Address;
import org.junit.Test;
import transaction_manager.TransactionController;
import transaction_manager.TransactionImpl;
import transaction_manager.raft.RaftTransactionManagerStub;

import java.sql.Time;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class RaftState {
    RaftTransactionManagerStub tms = new RaftTransactionManagerStub( "manager", "127.0.0.1:8081,127.0.0.1:8082");

    @Test
    public void readState(){
        System.out.println(tms.getExtendedState(0).toString());
    }

    @Test
    public void dummy(){
        HashMap<Timestamp<Long>, Long> map = new HashMap<>();
        MonotonicTimestamp ts1 = new MonotonicTimestamp(1L);
        MonotonicTimestamp ts2 = new MonotonicTimestamp(ts1);
        map.put(ts1, 1L);
        //map.remove(ts2);
        System.out.println(map.size());
        System.out.println(map.containsKey(ts2));
        System.out.println(map.containsKey(ts1));
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
