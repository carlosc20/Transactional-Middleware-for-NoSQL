package nosql;

public interface KeyValueDriver {

    byte[] read(byte[] key);
    void write(byte[] key, byte[] value);
    void delete(byte[] key);
    // scan?
}
