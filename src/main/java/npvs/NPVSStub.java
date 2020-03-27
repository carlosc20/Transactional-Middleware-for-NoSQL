package npvs;

import certifier.Timestamp;

import java.util.Map;

public class NPVSStub implements NPVS {
    @Override
    public void update(Map<byte[], byte[]> writeMap, Timestamp ts) {

    }

    @Override
    public byte[] read(byte[] key, Timestamp ts) {
        return new byte[0];
    }
}
