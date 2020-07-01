package npvs;

import certifier.Timestamp;
import utils.ByteArrayWrapper;

import java.util.Map;

public class FlushMessage {
    Map<ByteArrayWrapper, byte[]> writeMap;
    Timestamp timestamp;

    public FlushMessage(Map<ByteArrayWrapper, byte[]> ws, Timestamp ts){
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
