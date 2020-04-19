package nosql;

import utils.ByteArrayWrapper;

import java.util.Map;

public interface KeyValueDriver {

    byte[] read(byte[] key);
    void update(Map<ByteArrayWrapper,byte[]> writeMap);

}
