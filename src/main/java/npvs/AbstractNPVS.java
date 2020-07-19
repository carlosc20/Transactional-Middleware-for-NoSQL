package npvs;

import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import npvs.messaging.FlushMessage;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractNPVS implements NPVS<Long>{
    private HashSet<Timestamp<Long>> requestsMade;
    private Timestamp<Long> currentCommitTs;

    public AbstractNPVS(){
        this.requestsMade = new HashSet<>();
        this.currentCommitTs = new MonotonicTimestamp(0);
    }

    public abstract CompletableFuture<Void> putImpl(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> ts);

    public abstract CompletableFuture<NPVSReply> get(ByteArrayWrapper key, Timestamp<Long> ts);

    @Override
    public CompletableFuture<Void> put(FlushMessage flushMessage) {
        if(requestsMade.contains(flushMessage.getTransactionStartTimestamp()))
            return CompletableFuture.completedFuture(null);

        Timestamp<Long> incomingCurrentCommit = flushMessage.getCurrentTimestamp();
        if(incomingCurrentCommit.isAfter(currentCommitTs)){
            currentCommitTs.set(incomingCurrentCommit);
            requestsMade.clear();
        }
        return putImpl(flushMessage.getWriteMap(), flushMessage.getCurrentTimestamp());
    }
}
