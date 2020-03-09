package transaction_manager;

import java.util.List;

public interface Transaction {

    void flush();
    void write(byte[] key, byte[] value);
    void delete(byte[] key);
    byte[] read(byte[] key);
    List<byte[]> scan(List<byte[]> keys);
}
