package transaction_manager;
import java.util.List;

public interface Transaction {

    void write(byte[] key, byte[] value);
    void delete(byte[] key);
    byte[] read(byte[] key) throws OperationFailedException;
    List<byte[]> scan(List<byte[]> keys) throws OperationFailedException;

    Boolean commit();
}
