package nosql;

import utils.ByteArrayWrapper;

import java.util.List;
import java.util.Map;

public interface KeyValueDriver {

    byte[] read(byte[] key); // get
    List<byte[]> scan(List<byte[]> keyList);
    void update(Map<ByteArrayWrapper,byte[]> writeMap); //put

}
