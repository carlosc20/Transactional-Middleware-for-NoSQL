package nosql;

import java.util.Map;

public interface KeyValueDriver {

    byte[] read(byte[] key);
    void update(Map<byte[],byte[]> writeMap);

}
