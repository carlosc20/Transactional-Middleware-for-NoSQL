package transaction_manager.messaging;

import certifier.Timestamp;
import transaction_manager.BitWriteSet;
import utils.ByteArrayWrapper;

import java.util.Map;

public interface TransactionContentMessage<V> {
    BitWriteSet getWriteSet();
    Map<ByteArrayWrapper, byte[]> getWriteMap();
    Timestamp<V> getStartTimestamp();
}
