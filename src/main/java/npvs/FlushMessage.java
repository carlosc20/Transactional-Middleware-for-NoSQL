package npvs;

import certifier.Timestamp;

import java.util.Map;

public class FlushMessage {
    Map<byte[], byte[]> writeMap;
    Timestamp timestamp;

    public FlushMessage(Map<byte[], byte[]> ws, Timestamp ts){
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
