package npvs;

import certifier.Timestamp;

public interface NPVS {

    // API para TM
    void write(byte[] key, byte[] value, Timestamp ts);
    byte[] read(byte[] key);

    // API para outros nodos?
}
