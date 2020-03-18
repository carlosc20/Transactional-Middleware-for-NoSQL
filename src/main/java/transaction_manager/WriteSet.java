package transaction_manager;

public interface WriteSet {
    byte[] read(byte[] key);
    void write(byte[] key, byte[] value);
    void delete(byte[] key);
}
