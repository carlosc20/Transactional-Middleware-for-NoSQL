package transaction_manager.raft;

import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import nosql.KeyValueDriver;
import npvs.NPVS;
import npvs.messaging.FlushMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionContentMessage;
import transaction_manager.raft.snapshot.ExtendedState;
import transaction_manager.standalone.TransactionManagerImpl;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public abstract class RaftTransactionManager extends TransactionManagerImpl {
    private static final Logger LOG = LoggerFactory.getLogger(RaftTransactionManager.class);
    private Map<Timestamp<Long>, FlushAgainInfo> nonAckedFlushes;

    public RaftTransactionManager(long timestep, NPVS<Long> npvs, KeyValueDriver driver, ServersContextMessage scm) {
        super(timestep, npvs, driver, scm);
        this.nonAckedFlushes = new LinkedHashMap<>();
    }

    public abstract boolean isLeader();

    @Override
    public abstract void updateState(Timestamp<Long> startTimestamp, Timestamp<Long> commitTimestamp, CompletableFuture<Timestamp<Long>> cf);

    public void updateState(Timestamp<Long> startTimestamp, Timestamp<Long> commitTimestamp){
        getCertifier().update(commitTimestamp);
        removeFlush(startTimestamp);
    }

    @Override
    public CompletableFuture<Timestamp<Long>> tryCommit(TransactionContentMessage tc) {
        Timestamp<Long> commitTimestamp = certifierCommit(tc);
        if(commitTimestamp.toPrimitive() > 0) {
            LOG.info("Putting non acked TC={}", commitTimestamp.toPrimitive());
            FlushMessage flushMessage = new FlushMessage(tc.getWriteMap(), tc.getTimestamp(), getCertifier().getCurrentCommitTs());
            nonAckedFlushes.put(tc.getTimestamp(), new FlushAgainInfo(flushMessage, commitTimestamp));
            if(isLeader())
                return flush(flushMessage, commitTimestamp);
            else
                //return for a follower is irrelevant
                return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.completedFuture(new MonotonicTimestamp(-1));
    }

    public Map<Timestamp<Long>, FlushAgainInfo> getNonAckedFlushes() {
        return nonAckedFlushes;
    }

    public void removeFlush(Timestamp<Long> commitTimestamp){
        LOG.info("Removing non acked flush TC={}", commitTimestamp.toPrimitive());
        nonAckedFlushes.remove(commitTimestamp);
    }

    public void triggerNonAckedFlushes(){
        LOG.info("flushing non aknowledged writes, size = {}", nonAckedFlushes.size());
        nonAckedFlushes.forEach((k,v) -> flush(v.getFlushMessage(), v.getProvisionalCommitTimestamp()));
    }

    public void scheduleLeaderEvents(int periodicity, TimeUnit unit){
        //garbage collection
        LOG.info("Scheduling leader events");
        getExecutorService().schedule(()->{
            if(isLeader()){
                Timestamp<Long> lowWaterMark = getCertifier().getSafeToDeleteTimestamp();
            }
        }, periodicity, unit);
    }

    public void setCommitControlHandlerTimestamp(){
        getCommitControlHandler().setTimestamp(getCertifier().getCurrentCommitTs());
    }

    public void setState(ExtendedState es){
        super.setState(es.getStandaloneState());
        this.nonAckedFlushes = es.getNonAckedFlushs();
    }
}
