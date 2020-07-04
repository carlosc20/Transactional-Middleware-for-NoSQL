package transaction_manager.utils;

import java.util.Arrays;

public final class ByteArrayWrapper {
    private final byte[] data;

    public ByteArrayWrapper(byte[] data){
        if(data == null){
            throw new NullPointerException();
        }
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ByteArrayWrapper that = (ByteArrayWrapper) o;
        return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return new String(this.data);
    }
}
