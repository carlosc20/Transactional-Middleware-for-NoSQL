import transaction_manager.Transaction;
import transaction_manager.TransactionManager;
import transaction_manager.TransactionManagerImpl;

public class TMExample {

    public static void main(String[] args) {
        TransactionManager tm = new TransactionManagerImpl();
        Transaction tx = tm.startTransaction();
        tx.write("chave".getBytes(), "valor".getBytes());
        byte[] value = tx.read("chave".getBytes());
        System.out.println(new String(value));
        tm.tryCommit(tx);
    }

}
