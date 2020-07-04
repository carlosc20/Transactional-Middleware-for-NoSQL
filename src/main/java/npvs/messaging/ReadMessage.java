package npvs.messaging;

import certifier.Timestamp;
import transaction_manager.utils.ByteArrayWrapper;

public class ReadMessage {
    ByteArrayWrapper key;
    Timestamp<Long> ts;
    //temp
    int id;


    public ReadMessage(ByteArrayWrapper key, Timestamp<Long> ts, int id){
        this.key = key;
        this.ts = ts;
        this.id = id;
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

    public int getId() {
        return id;
    }
}
