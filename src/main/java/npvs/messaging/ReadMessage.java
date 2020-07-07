package npvs.messaging;

import certifier.Timestamp;
import transaction_manager.utils.ByteArrayWrapper;

public class ReadMessage {
    ByteArrayWrapper key;
    Timestamp<Long> ts;


    public ReadMessage(ByteArrayWrapper key, Timestamp<Long> ts){
        this.key = key;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "ReadMessage{" +
                "key=" + key.toString() +
                "timestamp= " + ts +
                '}';
    }

    public ByteArrayWrapper getKey() {
        return key;
    }

    public Timestamp<Long> getTs() {
        return ts;
    }

}
