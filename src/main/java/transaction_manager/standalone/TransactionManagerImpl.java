package transaction_manager.standalone;

import certifier.*;
import nosql.KeyValueDriver;
import npvs.NPVS;
import npvs.messaging.FlushMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.State;
import transaction_manager.TransactionManager;
import transaction_manager.TransactionManagerService;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionContentMessage;

import java.util.concurrent.CompletableFuture;

public class TransactionManagerImpl extends TransactionManagerService implements TransactionManager {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionManagerImpl.class);
    private Certifier<Long> certifier;
    private Timestamp<Long> lastNPVSCrash;

    public TransactionManagerImpl(long timestep, NPVS<Long> npvs, KeyValueDriver driver, ServersContextMessage scm){
        super(timestep, npvs, driver, scm);
        this.certifier = new IntervalCertifierImpl(timestep);
        this.lastNPVSCrash = new MonotonicTimestamp(-1);
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
            FlushMessage flushMessage = new FlushMessage(tc.getWriteMap(), tc.getTimestamp(), certifier.getCurrentCommitTs());
            return flush(flushMessage, commitTimestamp);
        } else {
            LOG.info("aborted a transaction with TS {}", tc.getTimestamp());
            return CompletableFuture.completedFuture(new MonotonicTimestamp(-1));
        }
    }

    public Timestamp<Long> certifierCommit(TransactionContentMessage tc){
        return certifier.commit(tc.getWriteSet(), tc.getTimestamp());
    }

    @Override
    public void updateState(Timestamp<Long> commitTimestamp, CompletableFuture<Timestamp<Long>> cf) {
        certifier.update(commitTimestamp);
        cf.complete(commitTimestamp);
    }

    public Certifier<Long> getCertifier() {
        return certifier;
    }

    public Timestamp<Long> getLastNPVSCrash() {
        return lastNPVSCrash;
    }

    public State getState(){
        return new State(this.certifier, this.lastNPVSCrash);
    }

    public void setState(State s){
        this.certifier = s.getCertifier();
        this.lastNPVSCrash = s.getLastNPVSCrash();
    }

}
