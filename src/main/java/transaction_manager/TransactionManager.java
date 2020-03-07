package transaction_manager;

public interface TransactionManager {

    Transaction startTransaction();
    void tryCommit(Transaction tx);
}
