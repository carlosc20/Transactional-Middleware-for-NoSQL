package certifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.utils.BitWriteSet;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;


public class IntervalCertifierImpl extends AbstractCertifier {
    private static final Logger LOG = LoggerFactory.getLogger(IntervalCertifierImpl.class);

    private final Timestamp<Long> currentStartTs;
    private final Timestamp<Long> provisionalCommitTs;
    private final LinkedList<CompletableFuture<Void>> startsOnWait;


    public IntervalCertifierImpl(long timestep) {
        super(timestep);
        currentStartTs = new MonotonicTimestamp(0);
        provisionalCommitTs = new MonotonicTimestamp(timestep);
        startsOnWait = new LinkedList<>();
    }

    public IntervalCertifierImpl(IntervalCertifierImpl certifier){
        super(certifier);
        currentStartTs = certifier.currentStartTs;
        provisionalCommitTs = certifier.provisionalCommitTs;
        startsOnWait = certifier.startsOnWait;
    }

    @Override
    public CompletableFuture<Timestamp<Long>> start() {
        if (currentCommitTs.toPrimitive() - currentStartTs.toPrimitive() == 1){
            CompletableFuture<Void> cf = new CompletableFuture<>();
            startsOnWait.add(cf);
            return cf.thenApply(x -> {
                currentStartTs.increment();
                return new MonotonicTimestamp(currentStartTs);
            });
        }
        currentStartTs.increment();
        transactionStarted(currentStartTs);
        return CompletableFuture.completedFuture(new MonotonicTimestamp(currentStartTs));
    }

    @Override
    public long truncateStartTS(Timestamp<Long> startTimestamp) {
        return truncateForGC(startTimestamp) + timestep;
    }

    @Override
    public long truncateForGC(Timestamp<Long> startTimestamp) {
        return startTimestamp.toPrimitive() / timestep * timestep;
    }

    @Override
    public Timestamp<Long> treatCommit(BitWriteSet newBws, Timestamp<Long> startTimestamp){
        if (isWritable(newBws, startTimestamp, provisionalCommitTs)) {
            history.put(provisionalCommitTs, newBws);
            Timestamp<Long> res = new MonotonicTimestamp(provisionalCommitTs);
            provisionalCommitTs.add(timestep);
            LOG.info("Transaction request with TS: {} commited to certifier. Aquired TC: {}", startTimestamp.toPrimitive(), provisionalCommitTs.toPrimitive());
            return res;
        }
        else
            return new MonotonicTimestamp(-1);
    }

    @Override
    public void update(Timestamp<Long> commitTimestamp) {
        currentCommitTs.set(commitTimestamp);
        currentStartTs.set(commitTimestamp);
        LOG.info("Updating certifier Timestamps -> currentCommitTs: {}", currentCommitTs.toPrimitive());
        if (startsOnWait.size() > 0)
            startsOnWait.forEach(x -> x.complete(null));
    }

    @Override
    public String toString() {
        return "IntervalCertifierImpl{" +
                "currentStartTs=" + currentStartTs.toPrimitive() + "\n" +
                ", provisionalCommitTs=" + provisionalCommitTs.toPrimitive() + "\n" +
                ", startsOnWait=" + startsOnWait.toString() + "\n" +
                ", timestep=" + timestep + "\n" +
                ", currentCommitTs=" + currentCommitTs.toPrimitive() + "\n" +
                ", lowWaterMark=" + lowWaterMark.toPrimitive() + "\n" +
                ", runningTransactions=" + runningTransactions + "\n" +
                ", history=" + getCardinality() +
                '}';
    }
}
