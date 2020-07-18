package transaction_manager.raft;

import certifier.Timestamp;
import nosql.KeyValueDriver;
import npvs.NPVS;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.raft.snapshot.ExtendedState;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class RaftTransactionManagerImpl extends RaftTransactionManager{
    private ScheduledExecutorService e = Executors.newScheduledThreadPool(8);
    private final AtomicLong leaderTerm = new AtomicLong(-1);
    private RequestHandler requestHandler;

    public RaftTransactionManagerImpl(long timestep, NPVS<Long> npvs, KeyValueDriver driver, ServersContextMessage scm, RequestHandler requestHandler) {
        super(timestep, npvs, driver, scm);
        this.requestHandler = requestHandler;
    }

    @Override
    public void updateStateByRaftOperation(Timestamp<Long> commitTimestamp) {
        requestHandler.applyOperation(TransactionManagerOperation.createUpdateState(commitTimestamp), null);
    }

    @Override
    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    public ExtendedState getExtendedState(){
        return new ExtendedState(getState(), getNonAckedFlushs());
    }

    /*
    public void scheduleLeaderEvents(){
        //garbage collection
        e.scheduleAtFixedRate(() ->)
    }

     */

}
