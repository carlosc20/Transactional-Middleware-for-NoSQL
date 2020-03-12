package certifier;

import transaction_manager.Transaction;

public interface Certifier {
    Timestamp start();
    Timestamp commit(Transaction ws);
    void update();
}
