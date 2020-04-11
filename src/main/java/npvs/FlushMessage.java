package npvs;

import utils.ByteArrayWrapper;

import java.util.Map;

public class FlushMessage {
    Map<ByteArrayWrapper, byte[]> writeMap;
    long timestamp;

    public FlushMessage(Map<ByteArrayWrapper, byte[]> ws, long ts){
        this.writeMap = ws;
        this.timestamp = ts;
    }

    @Override
    public String toString() {
        return "FlushMessage{" +
                "ws=" + writeMap +
                ", ts=" + timestamp +
                '}';
    }
}
