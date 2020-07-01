package transaction_manager;

import certifier.Timestamp;

import java.util.List;

public interface Transaction {

    void write(byte[] key, byte[] value);
    void delete(byte[] key);
    byte[] read(byte[] key);
    List<byte[]> scan(List<byte[]> keys);

    // ?
    void flush(Timestamp tc);
    Timestamp getStartTimestamp();
    BitWriteSet getWriteSet();
}
