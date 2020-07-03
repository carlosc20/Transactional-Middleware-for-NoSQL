package transaction_manager.utils;

import utils.ByteArrayWrapper;

public class KeyValue {
    private final ByteArrayWrapper key;
    private final byte[] value;

    public KeyValue(ByteArrayWrapper key, byte[] value){
        this.key = key;
        this.value = value;
    }

    public ByteArrayWrapper getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }
}
