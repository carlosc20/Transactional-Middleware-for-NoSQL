package certifier;

import transaction_manager.BitWriteSet;

public interface Certifier {
    Timestamp start();
    Timestamp commit(BitWriteSet ws);
    void update();
}
