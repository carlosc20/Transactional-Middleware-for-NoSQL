package transaction_manager.client_side;
import java.util.List;

public interface Transaction {

    void write(byte[] key, byte[] value);
    void delete(byte[] key);
    byte[] read(byte[] key);
    List<byte[]> scan(List<byte[]> keys);

    Boolean commit();
    //TODO rollback?
}