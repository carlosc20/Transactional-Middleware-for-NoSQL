package transaction_manager.raft.sofa_jraft;

import certifier.Timestamp;
import nosql.KeyValueDriver;
import npvs.NPVS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.State;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.raft.RaftTransactionManager;
import transaction_manager.raft.sofa_jraft.callbacks.CompletableClosure;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class RaftTransactionManagerImpl extends RaftTransactionManager {
    private static final Logger LOG = LoggerFactory.getLogger(RaftTransactionManagerImpl.class);
    private final AtomicLong leaderTerm = new AtomicLong(-1);
    private final RequestHandler requestHandler;

    public RaftTransactionManagerImpl(int batchTimeout , long timestep, NPVS<Long> npvs, KeyValueDriver driver, ServersContextMessage scm, RequestHandler requestHandler) {
        super(batchTimeout ,timestep, npvs, driver, scm);
        this.requestHandler = requestHandler;
    }

    @Override
    public void updateState(Timestamp<Long> startTimestamp, Timestamp<Long> commitTimestamp, List<CompletableFuture<Timestamp<Long>>> cfs) {
        LOG.info("Updating state TC: " + commitTimestamp.toPrimitive());
        requestHandler.applyOperation(StateMachineOperation.createUpdateState(startTimestamp, commitTimestamp, LocalDateTime.now()),
                new CompletableClosure<Void>(cfs));
    }

    @Override
    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    @Override
    public void garbageCollection(Timestamp<Long> lowWaterMark) {
        requestHandler.applyOperation(StateMachineOperation.createGarbageCollection(lowWaterMark), new CompletableClosure<>(null));
    }

    public void setTerm(long term){
        this.leaderTerm.set(term);
    }

    public State getExtendedState(){
        return getState();
    }
}
