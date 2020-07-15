import org.junit.Test;
import transaction_manager.TransactionController;
import transaction_manager.TransactionImpl;
import transaction_manager.standalone.TransactionManagerStub;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class FullTransactionTest {


    @Test
    public void justRead(){

    }

    @Test
    //BD must be empty
    public void writeThenRead() throws InterruptedException, ExecutionException {
        //new NPVSServer(20000).start();
        //new TransactionManagerServer(30000, 30001, 20000, "mongodb://127.0.0.1:27017", "testeLei", "teste1").start();

        TransactionManagerStub tms = new TransactionManagerStub( 12346, 30000);
        TransactionController transactionController = new TransactionController(12345, tms);
        transactionController.buildContext();
        TransactionImpl t1 = transactionController.startTransaction();
        TransactionImpl t11 = transactionController.startTransaction();

        byte[] writeKey1 = "melao".getBytes();
        byte[] writeKey2 = "meloa".getBytes();

        byte[] writeValue1 = "grande".getBytes();
        byte[] writeValue2 = "pequena".getBytes();

        t1.write(writeKey1, writeValue1);
        t1.write(writeKey2, writeValue2);
        boolean result1 = t1.commit();

        assertTrue("Shoudnt conflict", result1);

        //new transaction to read new values and update them
        TransactionImpl t2 = transactionController.startTransaction();
        //new transaction that will not see t2 changes
        TransactionImpl t22 = transactionController.startTransaction();

        assertTrue("Must be bigger", t2.getTs().isAfter(t1.getTs()));

        byte[] res1 = t2.read("melao".getBytes());
        byte[] res2 = t2.read("meloa".getBytes());

        byte[] newValue1 = "grande2".getBytes();
        byte[] newValue2 = "pequena2".getBytes();

        t2.write(writeKey1, newValue1);
        t2.write(writeKey2, newValue2);

        assertArrayEquals("Should be equal", res1,  writeValue1);
        assertArrayEquals("Should be equal", res2,  writeValue2);

        //read values older than t1 commit
        assertNull("Must be null", t11.read("melao".getBytes()));
        assertNull("Must be null", t11.read("meloa".getBytes()));

        t2.commit();

        //must read versions prior to t2 commit
        byte[] oldRes1 = t22.read("melao".getBytes());
        byte[] oldRes2 = t22.read("meloa".getBytes());


        assertArrayEquals("Should be equal", oldRes1,  writeValue1);
        assertArrayEquals("Should be equal", oldRes2,  writeValue2);

        //conflicting transaction with t2
        t22.write(writeKey1, oldRes1);
        t22.write(writeKey2, oldRes2);
        boolean result2 = t22.commit();

        assertFalse("Should conflict", result2);
    }
}
