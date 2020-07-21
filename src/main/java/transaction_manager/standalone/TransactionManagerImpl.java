package transaction_manager.standalone;

import certifier.*;
import nosql.KeyValueDriver;
import npvs.NPVS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.BatchTransactionManagerService;
import transaction_manager.State;
import transaction_manager.TransactionManager;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionContentMessage;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class TransactionManagerImpl extends BatchTransactionManagerService implements TransactionManager {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionManagerImpl.class);
    private Certifier<Long> certifier;
    private Timestamp<Long> lastLowWaterMark;

    public TransactionManagerImpl(int batchTimeout, long timestep, NPVS<Long> npvs, KeyValueDriver driver, ServersContextMessage scm){
        super(batchTimeout, npvs, driver, scm);
        this.certifier = new IntervalCertifierImpl(timestep);
        this.lastLowWaterMark = new MonotonicTimestamp(-1);
    }

    @Override
    public CompletableFuture<Timestamp<Long>> startTransaction(){
        return certifier.start();
    }

    @Override
    public CompletableFuture<Timestamp<Long>> tryCommit(TransactionContentMessage tc) {
        Timestamp<Long> commitTimestamp = certifierCommit(tc);
        if(commitTimestamp.toPrimitive() > 0) {
            LOG.info("Making transaction with TC: {} changes persist", commitTimestamp.toPrimitive());
            return flushInBatch(tc.getWriteMap(), tc.getTimestamp(), commitTimestamp, certifier.getCurrentCommitTs());
        } else {
            LOG.info("aborted a transaction with TS {}", tc.getTimestamp());
            return CompletableFuture.completedFuture(new MonotonicTimestamp(-1));
        }
    }

    public Timestamp<Long> certifierCommit(TransactionContentMessage tc){
        certifier.transactionEnded(tc.getTimestamp());
        return certifier.commit(tc.getWriteSet(), tc.getTimestamp());
    }

    @Override
    public void updateState(Timestamp<Long> startTimestamp, Timestamp<Long> commitTimestamp, List<CompletableFuture<Timestamp<Long>>> cfs) {
        certifier.setTombstone(LocalDateTime.now());
        certifier.update(commitTimestamp);
        cfs.forEach(cf -> cf.complete(commitTimestamp));
    }

    public Certifier<Long> getCertifier() {
        return certifier;
    }

    public void scheduleGarbageCollection(int periodicity, int forceGCInterval, TimeUnit unit, Consumer<Timestamp<Long>> forceGCCallback){
        LOG.info("Scheduling events");
        getExecutorService().scheduleAtFixedRate(() -> {
            try {
                Timestamp<Long> lowWaterMark = getCertifier().getSafeToDeleteTimestamp();
                if(lowWaterMark.toPrimitive() < 0)
                    lowWaterMark = new MonotonicTimestamp(0);
                if(lowWaterMark.equals(lastLowWaterMark)){
                    LOG.info("Duplicate lowWaterMark found");
                    lowWaterMark = getCertifier().getForceDeleteTimestamp(LocalDateTime.now(), forceGCInterval);
                    forceGCCallback.accept(lowWaterMark);
                }
                if(!lowWaterMark.equals(lastLowWaterMark)){
                    LOG.info("Sending evict message to npvs");
                    getNpvs().evict(lowWaterMark);
                }
                lastLowWaterMark.set(lowWaterMark);
            } catch ( Exception e ) {
               e.printStackTrace();}
        }, periodicity, periodicity, unit);
    }

    public void garbageCollection(Long newLowWaterMark){
        this.certifier.evictStoredWriteSets(newLowWaterMark);
    }

    public State getState(){
        return new State(this.certifier, this.lastLowWaterMark, getNonAckedFlushes());
    }

    public void setState(State s){
        this.certifier = s.getCertifier();
        this.lastLowWaterMark = s.getLastLowWaterMark();
        setNonAckedFlushes(s.getNonAckedFlushs());
    }

}
