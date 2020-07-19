import io.atomix.utils.net.Address;
import org.junit.Test;
import transaction_manager.TransactionController;
import transaction_manager.TransactionImpl;
import transaction_manager.raft.RaftTransactionManagerStub;

import java.util.concurrent.ExecutionException;

public class RaftState {
    RaftTransactionManagerStub tms = new RaftTransactionManagerStub( "manager", "127.0.0.1:8081,127.0.0.1:8082");

    @Test
    public void readState(){
        System.out.println(tms.getExtendedState().toString());
    }

    @Test
    public void write() throws ExecutionException, InterruptedException {
        TransactionController transactionController = new TransactionController(Address.from(23415), tms);
        transactionController.buildContext();
        TransactionImpl t = transactionController.startTransaction();
        byte[] writeKey1 = "melao".getBytes();
        byte[] writeKey2 = "meloa".getBytes();

        byte[] writeValue1 = "grande".getBytes();
        byte[] writeValue2 = "pequena".getBytes();

        t.write(writeKey1, writeValue1);
        t.write(writeKey2, writeValue2);
        boolean result1 = t.commit();
    }
}
