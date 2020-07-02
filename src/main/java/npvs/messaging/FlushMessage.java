package npvs.messaging;

import certifier.Timestamp;
import utils.ByteArrayWrapper;

import java.util.Map;

public class FlushMessage {
    Map<ByteArrayWrapper, byte[]> writeMap;
    Timestamp<Long> monotonicTimestamp;

    public FlushMessage(Map<ByteArrayWrapper, byte[]> ws, Timestamp<Long> ts){
        this.writeMap = ws;
        this.monotonicTimestamp = ts;
    }

    @Override
    public String toString() {
        return "FlushMessage{" +
                "ws=" + writeMap +
                ", ts=" + monotonicTimestamp +
                '}';
    }

    public Timestamp<Long> getMonotonicTimestamp() {
        return monotonicTimestamp;
    }

    public Map<ByteArrayWrapper, byte[]> getWriteMap() {
        return writeMap;
    }
}
