package transaction_manager.messaging;

import certifier.Timestamp;
import transaction_manager.BitWriteSet;
import utils.ByteArrayWrapper;

import java.util.Map;

public class TransactionContentMessageImpl implements TransactionContentMessage<Long> {

    //TODO na verdade pode apenas enviar o writeMap, mas a computação passa para o servidor
    private final Map<ByteArrayWrapper, byte[]> writeMap;
    private final Timestamp<Long> startTimestamp;
    private final BitWriteSet bws;

    public TransactionContentMessageImpl(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> startTimestamp){
        this.writeMap = writeMap;
        this.startTimestamp = startTimestamp;
        this.bws = new BitWriteSet();
        writeMap.keySet().forEach(k -> bws.add(k.getData()));
    }

    @Override
    public BitWriteSet getWriteSet() {
        return bws;
    }

    @Override
    public Map<ByteArrayWrapper, byte[]> getWriteMap() {
        return writeMap;
    }

    @Override
    public Timestamp<Long> getStartTimestamp() {
        return startTimestamp;
    }
}
