package npvs;

import utils.ByteArrayWrapper;

import java.util.Arrays;

public class ReadMessage {
    ByteArrayWrapper key;
    long ts;
    //temp
    int id;


    public ReadMessage(ByteArrayWrapper key, long ts, int id){
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
