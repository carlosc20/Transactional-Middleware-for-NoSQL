package transaction_manager.messaging;

import certifier.Timestamp;
import transaction_manager.utils.BitWriteSet;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.Map;

public class TransactionContentMessage {

    //TODO na verdade pode apenas enviar o writeMap, mas a computação passa para o servidor
    private final Map<ByteArrayWrapper, byte[]> writeMap;
    private final Timestamp<Long> startTimestamp;
    private final BitWriteSet bws;

    public TransactionContentMessage(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> startTimestamp){
        this.writeMap = writeMap;
        this.startTimestamp = startTimestamp;
        this.bws = new BitWriteSet();
        writeMap.keySet().forEach(k -> bws.add(k.getData()));
    }

    public BitWriteSet getWriteSet() {
        return bws;
    }

    public Map<ByteArrayWrapper, byte[]> getWriteMap() {
        return writeMap;
    }

    public Timestamp<Long> getStartTimestamp() {
        return startTimestamp;
    }
}
