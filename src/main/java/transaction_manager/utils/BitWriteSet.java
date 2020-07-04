package transaction_manager.utils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Set;

public class BitWriteSet implements Serializable {

    private final BitSet set;

    public BitWriteSet() {
        this(1024);
    }

    public BitWriteSet(int nbits) {
        this.set = new BitSet(nbits);
    }

    public BitWriteSet(Set<ByteArrayWrapper> keySet){
        this.set = new BitSet(1024);
        keySet.forEach(k -> add(k.getData()));
    }

    public void add(byte[] key) {
        int index = (Arrays.hashCode(key) & 0x7fffffff) % set.size();
        set.set(index, true);
    }

    public boolean intersects(BitWriteSet set) {
        if (set == null)
            return false;
        return this.set.intersects(set.getSet());
    }

    public BitSet getSet() {
        return set;
    }
}
