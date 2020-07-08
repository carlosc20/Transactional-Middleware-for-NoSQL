package transaction_manager.messaging;

import certifier.Timestamp;
import transaction_manager.utils.BitWriteSet;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.BitSet;
import java.util.Map;

public class TransactionContentMessage {

    //TODO na verdade pode apenas enviar o writeMap, mas a computação passa para o servidor
    private final Map<ByteArrayWrapper, byte[]> writeMap;
    private final Timestamp<Long> startTimestamp;
    private final byte[] bws;

    public TransactionContentMessage(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> startTimestamp){
        this.writeMap = writeMap;
        this.startTimestamp = startTimestamp;
        BitWriteSet bws = new BitWriteSet();
        writeMap.keySet().forEach(k -> bws.add(k.getData()));
        this.bws = bws.toByteArray();
    }

    public BitWriteSet getWriteSet() {
        return new BitWriteSet(bws);
    }

    public Map<ByteArrayWrapper, byte[]> getWriteMap() {
        return writeMap;
    }

    public Timestamp<Long> getStartTimestamp() {
        return startTimestamp;
    }
}
