package transaction_manager;

import transaction_manager.utils.BitWriteSet;

import java.util.List;

public interface Transaction {

    void write(byte[] key, byte[] value);
    void delete(byte[] key);
    byte[] read(byte[] key);
    List<byte[]> scan(List<byte[]> keys);

    Boolean tryCommit(BitWriteSet bws);
    //TODO rollback?
}
