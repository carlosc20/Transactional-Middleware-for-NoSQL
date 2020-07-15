package transaction_manager.messaging;

import certifier.Timestamp;
import transaction_manager.utils.BitWriteSet;
import transaction_manager.utils.ByteArrayWrapper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TransactionContentMessage implements Serializable {

    //TODO na verdade pode apenas enviar o writeMap, mas a computação passa para o servidor
    private final HashMap<ByteArrayWrapper, byte[]> writeMap;
    private final Timestamp<Long> timestamp;
    private final byte[] bws;

    public TransactionContentMessage(Timestamp<Long> timestamp){
        this.timestamp = timestamp;
        this.writeMap = null;
        this.bws = null;
    }

    public TransactionContentMessage(HashMap<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> timestamp){
        this.writeMap = writeMap;
        this.timestamp = timestamp;
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

    public Timestamp<Long> getTimestamp() {
        return timestamp;
    }
}
