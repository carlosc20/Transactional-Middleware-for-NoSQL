package certifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.utils.BitWriteSet;

import java.util.LinkedList;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;


public class IntervalCertifierImpl extends AbstractCertifier {
    private static final Logger LOG = LoggerFactory.getLogger(IntervalCertifierImpl.class);

    private final Timestamp<Long> currentStartTs;
    private final Timestamp<Long> provisionalCommitTs;
    private final Queue<CompletableFuture<Void>> startsOnWait;

    //TODO ver
    //não se vai esperar concorrência nestas estruturas -> Muitas escritas e poucas/raras leituras -> 1 thread
    //outras opções N threads:
    // ConcurrentSkipList -> matava a performance ao correr normalmente.
    // ConcurrentHashMap -> procura por todas as chaves na fase de GC

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
        return CompletableFuture.completedFuture(new MonotonicTimestamp(currentStartTs));
    }

    @Override
    public long truncateStartTS(long startTimestamp) {
        return startTimestamp / timestep * timestep + timestep;
    }

    @Override
    public Timestamp<Long> treatCommit(BitWriteSet newBws, Timestamp<Long> ts){
        long pcts = provisionalCommitTs.toPrimitive();
        if (isWritable(newBws, ts.toPrimitive(), pcts)) {
            history.put(pcts, newBws);
            provisionalCommitTs.add(timestep);
            LOG.info("Transaction request with TS: {} commited to certifier. Aquired TC: {}", ts, pcts);
            return new MonotonicTimestamp(pcts);
        }
        else
            return new MonotonicTimestamp(-1);
    }

    @Override
    public void update() {
        //TODO caso em que chega um commit mais adiantado (com gap)
        currentCommitTs.add(timestep);
        currentStartTs.setPrimitive(currentCommitTs.toPrimitive());
        LOG.info("Updating certifier Timestamps -> currentStartTs: {}, currentCommitTs: {}", currentStartTs.toPrimitive(), currentCommitTs.toPrimitive());
        if (startsOnWait.size() > 0){
            startsOnWait.forEach(x -> x.complete(null));
        }
    }

}
