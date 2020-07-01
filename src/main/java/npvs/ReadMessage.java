package npvs;

import certifier.Timestamp;
import utils.ByteArrayWrapper;

import java.util.Arrays;

public class ReadMessage {
    ByteArrayWrapper key;
    Timestamp ts;
    //temp
    int id;


    public ReadMessage(ByteArrayWrapper key, Timestamp ts, int id){
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
}
