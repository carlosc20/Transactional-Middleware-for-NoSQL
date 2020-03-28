package npvs;

import certifier.Timestamp;

import java.util.Arrays;

public class ReadMessage {
    byte[] key;
    Timestamp ts;


    public ReadMessage(byte[] key, Timestamp ts){
        this.key = key;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "ReadMessage{" +
                "key=" + Arrays.toString(key) +
                "timestamp= " + ts.toString() +
                '}';
    }
}
