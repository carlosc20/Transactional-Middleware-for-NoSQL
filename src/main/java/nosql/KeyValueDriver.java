package nosql;

import utils.ByteArrayWrapper;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface KeyValueDriver {

    CompletableFuture<byte[]> get(byte[] key);
    CompletableFuture<List<byte[]>> scan(Set<ByteArrayWrapper> keyList);
    CompletableFuture<Void> put(Map<ByteArrayWrapper,byte[]> writeMap);

}
