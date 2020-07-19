package transaction_manager.raft;

import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import nosql.KeyValueDriver;
import npvs.NPVS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionContentMessage;
import transaction_manager.raft.snapshot.ExtendedState;
import transaction_manager.standalone.TransactionManagerImpl;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public abstract class RaftTransactionManager extends TransactionManagerImpl {
    private static final Logger LOG = LoggerFactory.getLogger(RaftTransactionManager.class);
    private Map<Timestamp<Long>, Map<ByteArrayWrapper, byte[]>> nonAckedFlushs;

    public RaftTransactionManager(long timestep, NPVS<Long> npvs, KeyValueDriver driver, ServersContextMessage scm) {
        super(timestep, npvs, driver, scm);
        this.nonAckedFlushs = new LinkedHashMap<>();
    }

    public abstract boolean isLeader();

    @Override
    public abstract void updateState(Timestamp<Long> commitTimestamp, CompletableFuture<Timestamp<Long>> cf);

    @Override
    public CompletableFuture<Timestamp<Long>> tryCommit(TransactionContentMessage tc) {
        Timestamp<Long> commitTimestamp = certifierCommit(tc);
        if(commitTimestamp.toPrimitive() > 0) {
            nonAckedFlushs.put(commitTimestamp, tc.getWriteMap());
            if(isLeader())
                return flush(tc.getWriteMap(), commitTimestamp, getCertifier().getCurrentCommitTs());
            else
                //return for a follower is irrelevant
                return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.completedFuture(new MonotonicTimestamp(-1));
    }

    public Map<Timestamp<Long>, Map<ByteArrayWrapper, byte[]>> getNonAckedFlushs() {
        return nonAckedFlushs;
    }

    public void removeFlush(Timestamp<Long> startTimestamp){
        nonAckedFlushs.remove(startTimestamp);
    }

    public void triggerNonAckedFlushes(){
        LOG.info("flushing non aknowledged writes, size = {}", nonAckedFlushs.size());
        nonAckedFlushs.forEach((k,v) -> flush(v, k, getCertifier().getCurrentCommitTs()));
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

    public void setState(ExtendedState es){
        super.setState(es.getStandaloneState());
        this.nonAckedFlushs = es.getNonAckedFlushs();
    }
}
