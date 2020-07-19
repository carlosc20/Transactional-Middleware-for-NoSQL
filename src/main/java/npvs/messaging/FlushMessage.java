package npvs.messaging;

import certifier.Timestamp;
import transaction_manager.utils.ByteArrayWrapper;

import java.io.Serializable;
import java.util.Map;

public class FlushMessage implements Serializable {
    Timestamp<Long> transactionStartTimestamp;
    Timestamp<Long> currentTimestamp;
    Map<ByteArrayWrapper, byte[]> writeMap;

    public FlushMessage(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> transactionStartTimestamp, Timestamp<Long> currentTimestamp){
        this.transactionStartTimestamp = transactionStartTimestamp;
        this.currentTimestamp = currentTimestamp;
        this.writeMap = writeMap;
    }

    public FlushMessage(FlushMessage flushMessage){
        this.transactionStartTimestamp = flushMessage.transactionStartTimestamp;
        this.currentTimestamp = flushMessage.currentTimestamp;
        this.writeMap = flushMessage.writeMap;
    }

    public Timestamp<Long> getTransactionStartTimestamp() {
        return transactionStartTimestamp;
    }

    public Timestamp<Long> getCurrentTimestamp() {
        return currentTimestamp;
    }

    public Map<ByteArrayWrapper, byte[]> getWriteMap() {
        return writeMap;
    }

    public void setWriteMap(Map<ByteArrayWrapper, byte[]> writeMap) {
        this.writeMap = writeMap;
    }

    public FlushMessage getFlushMessage(){
        return this;
    }

    @Override
    public String toString() {
        return "FlushMessage{" +
                "transactionStartTimestamp=" + transactionStartTimestamp.toPrimitive() +
                ", currentTimestamp=" + currentTimestamp.toPrimitive() +
                ", writeMap=" + writeMap.toString() +
                '}';
    }
}
