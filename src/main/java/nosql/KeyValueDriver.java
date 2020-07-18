package nosql;

import certifier.Timestamp;
import nosql.messaging.GetMessage;
import nosql.messaging.ScanMessage;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface KeyValueDriver {

    CompletableFuture<byte[]> getWithoutTS(ByteArrayWrapper key);
    CompletableFuture<GetMessage> get(ByteArrayWrapper key);
    CompletableFuture<ScanMessage> scan(Set<ByteArrayWrapper> keyList);
    CompletableFuture<Void> put(Map<ByteArrayWrapper,byte[]> writeMap);
    CompletableFuture<Void> put(Timestamp<Long> timestamp);
    void drop();

}
