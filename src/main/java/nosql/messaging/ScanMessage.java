package nosql.messaging;

import certifier.Timestamp;

import java.util.List;

public class ScanMessage {
    private final List<byte[]> values;
    private final Timestamp<Long> ts;

    public ScanMessage(List<byte[]> values, Timestamp<Long> ts){
        this.values = values;
        this.ts = ts;
    }

    public List<byte[]> getValues() {
        return values;
    }

    public Timestamp<Long> getTs() {
        return ts;
    }
}
