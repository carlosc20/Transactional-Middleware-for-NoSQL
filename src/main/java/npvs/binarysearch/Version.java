package npvs.binarysearch;

import java.util.Arrays;

public class Version {
    byte[] value;
    long ts;

    public Version(byte[] value, long ts){
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
