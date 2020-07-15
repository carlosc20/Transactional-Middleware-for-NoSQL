package transaction_manager.raft;

import certifier.Timestamp;
import nosql.KeyValueDriver;
import npvs.NPVS;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.raft.snapshot.ExtendedState;

import java.util.concurrent.atomic.AtomicLong;

public class RaftTransactionManagerImpl extends RaftTransactionManager{
    private final AtomicLong leaderTerm = new AtomicLong(-1);
    private RaftTMService rtms;

    public RaftTransactionManagerImpl(long timestep, NPVS<Long> npvs, KeyValueDriver driver, ServersContextMessage scm) {
        super(timestep, npvs, driver, scm);
    }

    @Override
    public void updateStateByRaftOperation(Timestamp<Long> commitTimestamp) {
        //rtms.applyOperation(TransactionManagerOperation.createUpdateState(commitTimestamp), null);
    }

    @Override
    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    public ExtendedState getExtendedState(){
        return new ExtendedState(getState(), getNonAckedFlushs());
    }
}
