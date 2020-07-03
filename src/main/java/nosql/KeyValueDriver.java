package nosql;

import certifier.Timestamp;
import nosql.messaging.GetMessage;
import utils.ByteArrayWrapper;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface KeyValueDriver {

    CompletableFuture<byte[]> getWithoutTS(ByteArrayWrapper key);
    CompletableFuture<GetMessage> get(ByteArrayWrapper key);
    CompletableFuture<List<byte[]>> scan(Set<ByteArrayWrapper> keyList);
    CompletableFuture<Void> put(Map<ByteArrayWrapper,byte[]> writeMap, Timestamp<Long> timestamp);

}
