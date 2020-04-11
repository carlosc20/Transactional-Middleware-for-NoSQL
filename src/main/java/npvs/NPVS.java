package npvs;

import utils.ByteArrayWrapper;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface NPVS {

    void update(Map<ByteArrayWrapper,byte[]> writeMap, long ts);
    CompletableFuture<byte[]> read(ByteArrayWrapper key, long ts);
}
