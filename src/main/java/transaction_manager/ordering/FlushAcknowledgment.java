package transaction_manager.ordering;

import certifier.Timestamp;

import java.util.concurrent.CompletableFuture;

public class FlushAcknowledgment {
    private CompletableFuture<Void> cf;
    private Timestamp<Long> commitTs;

    public FlushAcknowledgment(CompletableFuture<Void> cf, Timestamp<Long> commitTs) {
        this.cf = cf;
        this.commitTs = commitTs;
    }

    public CompletableFuture<Void> getCf() {
        return cf;
    }

    public Timestamp<Long> getCommitTs() {
        return commitTs;
    }
}
