package npvs.binarysearch;

import certifier.Timestamp;

import java.util.Arrays;

public class Version {
    byte[] value;
    Timestamp<Long> ts;

    public Version(byte[] value, Timestamp<Long> ts){
        this.value = value;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "Version{" +
                "value=" + Arrays.toString(value) +
                ", ts=" + ts +
                '}';
    }
}
