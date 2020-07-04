package certifier;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.utils.BitWriteSet;


public class CertifierImpl implements Certifier<Long> {
    private static final Logger LOG = LoggerFactory.getLogger(CertifierImpl.class);

    private final Long timestep;

    private final Timestamp<Long> currentStartTs;
    private final Timestamp<Long> provisionalCommitTs;
    private final Timestamp<Long> currentCommitTs;
    private final Timestamp<Long> lowWaterMark;

    //não se vai esperar concorrência nestas estruturas -> Muitas escritas e poucas/raras leituras -> 1 thread
    //outras opções N threads:
    // ConcurrentSkipList -> matava a performance ao correr normalmente.
    // ConcurrentHashMap -> procura por todas as chaves na fase de GC
    private final LinkedHashMap<Long, Integer> runningTransactions;

    private final HashMap<Long, BitWriteSet> history;

    public CertifierImpl(long timestep) {
        this.timestep = timestep;
        currentStartTs = new MonotonicTimestamp(0);
        provisionalCommitTs = new MonotonicTimestamp(timestep);
        currentCommitTs = new MonotonicTimestamp(timestep);
        lowWaterMark = new MonotonicTimestamp(-1);
        runningTransactions = new LinkedHashMap<>();
        history = new HashMap<>();
    }

    public CertifierImpl(CertifierImpl certifier){
        timestep = certifier.timestep;
        currentStartTs = certifier.currentStartTs;
        provisionalCommitTs = certifier.provisionalCommitTs;
        currentCommitTs = certifier.currentCommitTs;
        lowWaterMark = certifier.lowWaterMark;
        runningTransactions = new LinkedHashMap<>(certifier.runningTransactions);
        history = new HashMap<>(certifier.history);
    }

    @Override
    public Timestamp<Long> start() {
        //spin can be unsafe
        while(currentCommitTs.toPrimitive() - currentStartTs.toPrimitive() == 1){
            //spin
        }
        currentStartTs.increment();
        return new MonotonicTimestamp(currentStartTs);
    }

    private boolean isWritable(BitWriteSet newBws, long startTimestamp, long provisionalCommitTs){
        for (long i = startTimestamp; i < provisionalCommitTs; i += timestep) {
            BitWriteSet oldBws = history.get(i);
            if (newBws.intersects(oldBws)) {
                LOG.debug("Transaction conflicted on timestamp({})", i);
                return false;
            }
        }
        return true;
    }

    @Override
    public Timestamp<Long> commit(BitWriteSet newBws, Timestamp<Long> ts) {
        if (ts.isBefore(lowWaterMark)) {
            LOG.debug("Received transaction request with a timestamp ({}) already garbage collected", ts);
            return new MonotonicTimestamp(-1L);
        }
        long pcts = provisionalCommitTs.toPrimitive();
        if (isWritable(newBws, ts.toPrimitive(), pcts)) {
            history.put(pcts, newBws);
            provisionalCommitTs.add(timestep);
            return new MonotonicTimestamp(pcts);
        }
        else
            return new MonotonicTimestamp(-1);
    }

    @Override
    //ASSUME causal order, implementar caso necessário
    public void update() {
        currentCommitTs.add(timestep);
        currentStartTs.setPrimitive(currentCommitTs.toPrimitive());
    }

    @Override
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


    @Override
    public void evictStoredWriteSets(Long newLowWaterMark){
        for(long i = lowWaterMark.toPrimitive(); i <= newLowWaterMark; i += timestep){
            this.history.remove(i);
            this.runningTransactions.remove(i);
        }
        this.lowWaterMark.setPrimitive(newLowWaterMark);
    }

    @Override
    public Timestamp<Long> getCurrentCommitTs() {
        return new MonotonicTimestamp(currentCommitTs);
    }

}
