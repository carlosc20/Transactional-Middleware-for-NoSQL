package npvs;

import certifier.Timestamp;

import java.util.Map;

public interface NPVS {

    void update(Map<byte[],byte[]> writeMap, Timestamp ts);
    byte[] read(byte[] key, Timestamp ts);
}
