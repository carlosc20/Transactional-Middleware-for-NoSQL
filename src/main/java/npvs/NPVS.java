package npvs;

import certifier.Timestamp;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface NPVS {

    void update(Map<byte[],byte[]> writeMap, Timestamp ts);
    CompletableFuture<byte[]> read(byte[] key, Timestamp ts);
}
