package nosql.messaging;

import certifier.Timestamp;

public class GetMessage {
    private final byte[] value;
    private final Timestamp<Long> ts;

    public GetMessage(byte[] value, Timestamp<Long> ts){
        this.value = value;
        this.ts = ts;
    }

    public byte[] getValue() {
        return value;
    }

    public Timestamp<Long> getTs() {
        return ts;
    }
}
