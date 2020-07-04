package npvs.messaging;

import certifier.Timestamp;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.Map;

public class FlushMessage {
    Map<ByteArrayWrapper, byte[]> writeMap;
    Timestamp<Long> ts;

    public FlushMessage(Map<ByteArrayWrapper, byte[]> ws, Timestamp<Long> ts){
        this.writeMap = ws;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "FlushMessage{" +
                "ws=" + writeMap +
                ", ts=" + ts +
                '}';
    }

    public Timestamp<Long> getTs() {
        return ts;
    }

    public Map<ByteArrayWrapper, byte[]> getWriteMap() {
        return writeMap;
    }
}
