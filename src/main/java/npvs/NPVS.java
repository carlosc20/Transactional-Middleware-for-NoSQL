package npvs;

import certifier.Timestamp;
import utils.ByteArrayWrapper;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface NPVS {

    void update(Map<ByteArrayWrapper,byte[]> writeMap, Timestamp ts);
    CompletableFuture<byte[]> read(ByteArrayWrapper key, Timestamp ts);
}
