package certifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.utils.BitWriteSet;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractCertifier implements Certifier<Long>{
    private static final Logger LOG = LoggerFactory.getLogger(AbstractCertifier.class);

    long timestep;

    final Timestamp<Long> currentCommitTs;
    final Timestamp<Long> lowWaterMark;
    final Timestamp<Long> lastTombStone;

    final LinkedHashMap<Timestamp<Long>, RunningState> runningTransactions;

    final HashMap<Timestamp<Long>, BitWriteSet> history;

    public AbstractCertifier(long timestep){
        this.timestep = timestep;
        currentCommitTs = new MonotonicTimestamp(0);
        lowWaterMark = new MonotonicTimestamp(-1);
        runningTransactions = new LinkedHashMap<>();
        history = new HashMap<>();
        lastTombStone = new MonotonicTimestamp(timestep);
    }

    public AbstractCertifier(AbstractCertifier certifier){
        timestep = certifier.timestep;
        currentCommitTs = certifier.currentCommitTs;
        lowWaterMark = certifier.lowWaterMark;
        runningTransactions = new LinkedHashMap<>(certifier.runningTransactions);
        history = new HashMap<>(certifier.history);
        lastTombStone = certifier.lastTombStone;
    }

    public abstract CompletableFuture<Timestamp<Long>> start();

    public abstract long truncateStartTS(Timestamp<Long> startTimestamp);

    public abstract long truncateForGC(Timestamp<Long> startTimestamp);

    public abstract Timestamp<Long> treatCommit(BitWriteSet newBws, Timestamp<Long> ts);

    public abstract void update(Timestamp<Long> commitTimestamp);

    protected boolean isWritable(BitWriteSet newBws, Timestamp<Long> startTimestamp, Timestamp<Long> commitTs){
        if (newBws.getSet().isEmpty())
            return true;
        for (long i = truncateStartTS(startTimestamp); i < commitTs.toPrimitive(); i += timestep) {
            BitWriteSet oldBws = history.get(i);
            if (newBws.intersects(oldBws)) {
                LOG.info("Transaction with TS: {} conflicted on TC: {}", startTimestamp, i);
                return false;
            }
        }
        return true;
    }

    public Timestamp<Long> commit(BitWriteSet newBws, Timestamp<Long> ts) {
        if (ts.isBefore(lowWaterMark)) {
            LOG.info("Received transaction request with a TS: {} already garbage collected", ts);
            return new MonotonicTimestamp(-1);
        }
        return treatCommit(newBws, ts);
    }

    @Override
    public void transactionStarted(Timestamp<Long> startTimestamp) {
        Timestamp<Long> truncated = new MonotonicTimestamp(truncateForGC(startTimestamp));
        LOG.info("transaction started truncated={}", truncated.toPrimitive());
        this.runningTransactions.putIfAbsent(truncated, new RunningState());
        this.runningTransactions.get(truncated).addTransaction();
    }

    @Override
    public void transactionEnded(Timestamp<Long> startTimestamp){
        Timestamp<Long> truncated = new MonotonicTimestamp(truncateForGC(startTimestamp));
        LOG.info("transaction commited={}", truncated.toPrimitive());
        this.runningTransactions.get(truncated).removeTransaction();
    }

    @Override
    public void setTombstone(LocalDateTime value){
        LOG.info("set Tombstone={}", currentCommitTs.toPrimitive());
        this.runningTransactions.get(currentCommitTs).setTombstone(value);
    }

    @Override
    public Timestamp<Long> getSafeToDeleteTimestamp(){
        Timestamp<Long> newLowWaterMark = lowWaterMark;
        for(Map.Entry<Timestamp<Long>, RunningState> entry : runningTransactions.entrySet()){
            RunningState runningState = entry.getValue();
            if(!runningState.isCleared()) {
                return new MonotonicTimestamp(newLowWaterMark);
            }
            else
                newLowWaterMark = entry.getKey();
        }
        LOG.info("Normal GC newLowWaterMark = {}", newLowWaterMark.toPrimitive());
        return new MonotonicTimestamp(newLowWaterMark);
    }


    @Override
    public void evictStoredWriteSets(Long newLowWaterMark){
        for(long i = lowWaterMark.toPrimitive(); i <= newLowWaterMark; i += timestep){
            this.history.remove(i);
            this.runningTransactions.remove(i);
        }
        this.lowWaterMark.setPrimitive(newLowWaterMark);
        LOG.info("GC performed newLowWaterMark = {}", this.lowWaterMark.toPrimitive());
    }


    public Timestamp<Long> getForceDeleteTimestamp(LocalDateTime eventTime, long intervalSec){
        Timestamp<Long> newLowWaterMark = lowWaterMark;
        for(Map.Entry<Timestamp<Long>, RunningState> entry : runningTransactions.entrySet()){
            LocalDateTime tombstone = entry.getValue().getTombstone();
            if(tombstone != null){
                long interval = Duration.between(tombstone, eventTime).getSeconds();
                if(interval > intervalSec)
                    newLowWaterMark = entry.getKey();
            }
        }
        LOG.info("Force GC newLowWaterMark = {}", newLowWaterMark.toPrimitive());
        return newLowWaterMark;
    }

    public Timestamp<Long> getCurrentCommitTs() {
        return new MonotonicTimestamp(currentCommitTs);
    }

    public String getCardinality(){
        StringBuilder sb = new StringBuilder();
        sb.append("[ ");
        history.forEach((k, v) -> sb.append("k: ").append(k).append(" v: ").append(v.getSet().cardinality()).append(", "));
        sb.append(" ]");
        return sb.toString();
    }
}
