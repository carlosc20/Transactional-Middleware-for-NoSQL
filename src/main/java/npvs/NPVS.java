package npvs;

import certifier.Timestamp;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface NPVS {

    void update(Map<byte[],byte[]> writeMap, long ts);
    CompletableFuture<byte[]> read(byte[] key, long ts);
}
