package certifier;

import transaction_manager.BitWriteSet;

public interface Certifier {
    long start();
    long commit(BitWriteSet ws, long ts);
    void update();
}
