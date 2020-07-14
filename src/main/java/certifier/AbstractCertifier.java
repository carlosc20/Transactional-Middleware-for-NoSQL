package certifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.utils.BitWriteSet;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractCertifier implements Certifier<Long>{
    private static final Logger LOG = LoggerFactory.getLogger(AbstractCertifier.class);

    long timestep;

    final Timestamp<Long> currentCommitTs;
    final Timestamp<Long> lowWaterMark;

    final LinkedHashMap<Long, Integer> runningTransactions;

    final HashMap<Long, BitWriteSet> history;

    public AbstractCertifier(long timestep){
        this.timestep = timestep;
        currentCommitTs = new MonotonicTimestamp(0);
        lowWaterMark = new MonotonicTimestamp(-1);
        runningTransactions = new LinkedHashMap<>();
        history = new HashMap<>();
    }

    public AbstractCertifier(AbstractCertifier certifier){
        timestep = certifier.timestep;
        currentCommitTs = certifier.currentCommitTs;
        lowWaterMark = certifier.lowWaterMark;
        runningTransactions = new LinkedHashMap<>(certifier.runningTransactions);
        history = new HashMap<>(certifier.history);
    }

    public abstract CompletableFuture<Timestamp<Long>> start();

    public abstract long truncateStartTS(long startTimestamp);

    public abstract Timestamp<Long> treatCommit(BitWriteSet newBws, Timestamp<Long> ts);

    public abstract void update();

    protected boolean isWritable(BitWriteSet newBws, long startTimestamp, long commitTs){
        // ts / timestamp -> divisão com long é truncada com as casas décimais do timestamp.
        for (long i = truncateStartTS(startTimestamp); i < commitTs; i += timestep) {
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

    public Timestamp<Long> getSafeToDeleteTimestamp(){
        long newLowWaterMark = lowWaterMark.toPrimitive();
        for(Map.Entry<Long, Integer> entry : runningTransactions.entrySet()){
            int numTransactionsRunning = entry.getValue();
            if(numTransactionsRunning != 0)
                return new MonotonicTimestamp(newLowWaterMark);
            else
                newLowWaterMark = entry.getKey();
        }
        return new MonotonicTimestamp(newLowWaterMark);
    }


    public void evictStoredWriteSets(Long newLowWaterMark){
        for(long i = lowWaterMark.toPrimitive(); i <= newLowWaterMark; i += timestep){
            this.history.remove(i);
            this.runningTransactions.remove(i);
        }
        this.lowWaterMark.setPrimitive(newLowWaterMark);
    }

    public Timestamp<Long> getCurrentCommitTs() {
        return new MonotonicTimestamp(currentCommitTs);
    }

}
