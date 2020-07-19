package npvs;

import certifier.Timestamp;
import npvs.messaging.FlushMessage;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.concurrent.CompletableFuture;

public interface NPVS<V> {

    CompletableFuture<Void> put(FlushMessage flushMessage);
    CompletableFuture<NPVSReply> get(ByteArrayWrapper key, Timestamp<V> ts);
}
