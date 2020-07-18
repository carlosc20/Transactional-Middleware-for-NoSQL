package transaction_manager.raft;

import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import nosql.KeyValueDriver;
import npvs.NPVS;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionContentMessage;
import transaction_manager.raft.snapshot.ExtendedState;
import transaction_manager.standalone.TransactionManagerImpl;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class RaftTransactionManager extends TransactionManagerImpl {
    private Map<Long, Map<ByteArrayWrapper, byte[]>> nonAckedFlushs;

    public RaftTransactionManager(long timestep, NPVS<Long> npvs, KeyValueDriver driver, ServersContextMessage scm) {
        super(timestep, npvs, driver, scm);
        this.nonAckedFlushs = new HashMap<>();
    }

    @Override
    public CompletableFuture<Timestamp<Long>> tryCommit(TransactionContentMessage tc) {
        Timestamp<Long> commitTimestamp = certifierCommit(tc);
        if(commitTimestamp.toPrimitive() > 0) {
            nonAckedFlushs.put(commitTimestamp.toPrimitive(), tc.getWriteMap());
            if(isLeader())
                return flush(tc, commitTimestamp, getCertifier().getCurrentCommitTs()).thenApply(x -> commitTimestamp);
            else
                //return for a follower is irrelevant
                return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.completedFuture(new MonotonicTimestamp(-1));
    }

    public Map<Long, Map<ByteArrayWrapper, byte[]>> getNonAckedFlushs() {
        return nonAckedFlushs;
    }

    @Override
    public void updateState(Timestamp<Long> commitTimestamp) {
       updateStateByRaftOperation(commitTimestamp);
    }

    public abstract void updateStateByRaftOperation(Timestamp<Long> commitTimestamp);

    public abstract boolean isLeader();

    public void removeFlush(Timestamp<Long> startTimestamp){
        nonAckedFlushs.remove(startTimestamp.toPrimitive());
    }

    /*
    public void triggerNonAckedFlushes(){
        nonAckedFlushs.
    }

     */

    public void setState(ExtendedState es){
        super.setState(es.getStandaloneState());
        this.nonAckedFlushs = es.getNonAckedFlushs();
    }

}
