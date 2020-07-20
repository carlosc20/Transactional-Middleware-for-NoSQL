package transaction_manager.raft;

import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import nosql.KeyValueDriver;
import npvs.NPVS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.State;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionContentMessage;
import transaction_manager.standalone.TransactionManagerImpl;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public abstract class RaftTransactionManager extends TransactionManagerImpl {
    private static final Logger LOG = LoggerFactory.getLogger(RaftTransactionManager.class);

    public RaftTransactionManager(long timestep, NPVS<Long> npvs, KeyValueDriver driver, ServersContextMessage scm) {
        super(timestep, npvs, driver, scm);
    }

    public abstract boolean isLeader();

    @Override
    public abstract void updateState(Timestamp<Long> startTimestamp, Timestamp<Long> commitTimestamp, List<CompletableFuture<Timestamp<Long>>> cf);

    public void updateState(Timestamp<Long> startTimestamp, Timestamp<Long> commitTimestamp, LocalDateTime leaderTime){
        getCertifier().update(commitTimestamp);
        removeFlush(startTimestamp);
        getCertifier().setTombstone(commitTimestamp, leaderTime);
    }

    @Override
    public CompletableFuture<Timestamp<Long>> tryCommit(TransactionContentMessage tc) {
        Timestamp<Long> commitTimestamp = getCertifier().commit(tc.getWriteSet(), tc.getTimestamp());
        if(commitTimestamp.toPrimitive() > 0) {
            LOG.info("Putting non acked TC={}", commitTimestamp.toPrimitive());
            if(isLeader())
                return flushInBatch(tc.getWriteMap(), tc.getTimestamp(), commitTimestamp, getCertifier().getCurrentCommitTs());
            else
                //return for a follower is irrelevant
                return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.completedFuture(new MonotonicTimestamp(-1));
    }

    public void setCommitControlHandlerTimestamp(){
        getCommitControlHandler().setTimestamp(getCertifier().getCurrentCommitTs());
    }

    public void setState(State s){
        super.setState(s);
    }
}
