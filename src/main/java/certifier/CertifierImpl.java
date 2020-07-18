package certifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.utils.BitWriteSet;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

public class CertifierImpl extends AbstractCertifier {
    private static final Logger LOG = LoggerFactory.getLogger(CertifierImpl.class);
    private final Timestamp<Long> currentStartTs;

    public CertifierImpl() {
        super(1);
        currentStartTs = new MonotonicTimestamp(0);
    }

    public CertifierImpl(CertifierImpl certifier){
        super(certifier);
        currentStartTs = certifier.currentStartTs;
    }

    @Override
    public CompletableFuture<Timestamp<Long>> start() {
        return CompletableFuture.completedFuture(new MonotonicTimestamp(currentStartTs));
    }

    @Override
    public long truncateStartTS(long startTimestamp) {
        return startTimestamp;
    }

    @Override
    public Timestamp<Long> treatCommit(BitWriteSet newBws, Timestamp<Long> ts){
        long ct = currentCommitTs.toPrimitive();
        if (isWritable(newBws, ts.toPrimitive(), ct)) {
            history.put(ct, newBws);
            LOG.info("Transaction request with TS: {} commited to certifier. Aquired TC: {}", ts, ct);
            return new MonotonicTimestamp(ct);
        }
        else
            return new MonotonicTimestamp(-1);
    }

    @Override
    public void update(Timestamp<Long> commitTimestamp) {
        currentCommitTs.set(commitTimestamp);
        LOG.info("Updating certifier Timestamps -> currentStartTs: {}, currentCommitTs: {}", currentStartTs.toPrimitive(), currentCommitTs.toPrimitive());
    }
}
