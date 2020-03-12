package npvs;

import certifier.Timestamp;

public interface NPVS {

    void write(byte[] key, byte[] value, Timestamp ts);
    byte[] read(byte[] key, Timestamp ts);
}
