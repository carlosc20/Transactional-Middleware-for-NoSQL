package transaction_manager.raft;

import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import nosql.KeyValueDriver;
import npvs.NPVS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionContentMessage;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public abstract class AsyncResponseRaftTransactionManager extends RaftTransactionManager {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncResponseRaftTransactionManager.class);


    public AsyncResponseRaftTransactionManager(int batchTimeout, long timestep, NPVS<Long> npvs, KeyValueDriver driver, ServersContextMessage scm) {
        super(batchTimeout, timestep, npvs, driver, scm);
    }

    @Override
    public abstract void updateState(Timestamp<Long> startTimestamp, Timestamp<Long> commitTimestamp, List<CompletableFuture<Timestamp<Long>>> cf);
    
    /*
    Response to the client doesnt wait for the the following flushes
     */
    @Override
    public CompletableFuture<Timestamp<Long>> tryCommit(TransactionContentMessage tc) {
        return CompletableFuture.supplyAsync(() -> getCertifier().commit(tc.getWriteSet(), tc.getTimestamp()), singleExecutor)
            .thenCompose((provisionalCommitTimestamp) -> {
                if(provisionalCommitTimestamp.toPrimitive() > 0) {
                    LOG.info("Putting non acked TC={}", provisionalCommitTimestamp.toPrimitive());
                    //tc.getTimestamp == startTimestamp in this case
                    if(isLeader()) {
                        flushInBatch(tc.getWriteMap(), tc.getTimestamp(), provisionalCommitTimestamp, getCertifier().getCurrentCommitTs());
                        return CompletableFuture.completedFuture(provisionalCommitTimestamp);
                    }
                    else
                        //return for a follower is irrelevant
                        return CompletableFuture.completedFuture(null);
                }
                return CompletableFuture.completedFuture(new MonotonicTimestamp(-1));
                });
    }

}
