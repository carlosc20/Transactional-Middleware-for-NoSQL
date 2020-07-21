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

    public RaftTransactionManager(int batchTimeout, long timestep, NPVS<Long> npvs, KeyValueDriver driver, ServersContextMessage scm) {
        super(batchTimeout, timestep, npvs, driver, scm);
    }

    public abstract boolean isLeader();

    public abstract void garbageCollection(Timestamp<Long> lowWaterMark);

    @Override
    public abstract void updateState(Timestamp<Long> startTimestamp, Timestamp<Long> commitTimestamp, List<CompletableFuture<Timestamp<Long>>> cf);

    public void updateState(Timestamp<Long> startTimestamp, Timestamp<Long> commitTimestamp, LocalDateTime leaderTime){
        getCertifier().setTombstone(leaderTime);
        getCertifier().update(commitTimestamp);
        removeFlush(startTimestamp);
    }

    @Override
    public CompletableFuture<Timestamp<Long>> tryCommit(TransactionContentMessage tc) {
        Timestamp<Long> provisionalCommitTimestamp = getCertifier().commit(tc.getWriteSet(), tc.getTimestamp());
        if(provisionalCommitTimestamp.toPrimitive() > 0) {
            LOG.info("Putting non acked TC={}", provisionalCommitTimestamp.toPrimitive());
            //tc.getTimestamp == startTimestamp in this case
            if(isLeader())
                return flushInBatch(tc.getWriteMap(), tc.getTimestamp(), provisionalCommitTimestamp, getCertifier().getCurrentCommitTs());
            else
                //return for a follower is irrelevant
                return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.completedFuture(new MonotonicTimestamp(-1));
    }

    public void abort(Timestamp<Long> startTimestamp){
        getCertifier().transactionEnded(startTimestamp);
    }

    public void setState(State s){
        super.setState(s);
    }
}
