import org.junit.Test;
import transaction_manager.Transaction;
import transaction_manager.TransactionController;

public class FullTransactionTest {

    @Test
    public void writeThenRead(){
        TransactionController transactionController = new TransactionController(12345, 30000);
        transactionController.buildContext();

        Transaction t1 = transactionController.startTransaction();


    }
}
