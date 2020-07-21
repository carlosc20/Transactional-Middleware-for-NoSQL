package transaction_manager.raft.sofa_jraft;

import certifier.Timestamp;
import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;
import nosql.KeyValueDriver;
import npvs.NPVS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.State;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionContentMessage;
import transaction_manager.raft.sofa_jraft.callbacks.CompletableClosure;
import transaction_manager.raft.sofa_jraft.callbacks.TransactionClosure;
import transaction_manager.raft.snapshot.StateSnapshot;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static transaction_manager.raft.sofa_jraft.StateMachineOperation.*;

public class ManagerStateMachine extends StateMachineAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(ManagerStateMachine.class);

    private final RaftTransactionManagerImpl transactionManager;
    private final ExecutorService singleExecutor;

    public ManagerStateMachine(int batchTimeout, long timestep, NPVS<Long> npvs, KeyValueDriver driver, ServersContextMessage scm, RequestHandler requestHandler){
        super();
        this.singleExecutor = Executors.newSingleThreadExecutor();
        this.transactionManager = new RaftTransactionManagerImpl(batchTimeout,timestep, npvs, driver, scm, requestHandler);
    }

    //debug
    public State getExtendedState(){
        return transactionManager.getExtendedState();
    }

    public boolean isLeader() {
        return transactionManager.isLeader();
    }

    public ServersContextMessage getServersContext(){
        return transactionManager.getServersContext();
    }


    public void onApply(final Iterator iter) {
        while (iter.hasNext()) {
            StateMachineOperation stateMachineOperation = null;

            TransactionClosure closure = null;
            if (iter.done() != null) {
                // This task is applied by this node, get value from closure to avoid additional parsing.
                closure = (TransactionClosure) iter.done();
                stateMachineOperation = closure.getStateMachineOperation();
            } else {
                // Have to parse FetchAddRequest from this user log.
                final ByteBuffer data = iter.getData();
                try {
                    stateMachineOperation = SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(
                            data.array(), StateMachineOperation.class.getName());
                } catch (final CodecException e) {
                    LOG.error("Fail to decode TransactionManagerOperation", e);
                }
            }
            applyOperation(stateMachineOperation, closure);
            iter.next();
        }
    }

    private void applyOperation(StateMachineOperation stateMachineOperation, TransactionClosure closure){
        if (stateMachineOperation != null) {
            switch (stateMachineOperation.getOp()) {
                case START_TXN:
                    transactionManager.startTransaction().thenAccept(res -> treatClosure(res, closure));
                    break;
                case COMMIT:
                    final TransactionContentMessage tcm = stateMachineOperation.getTcm();
                    transactionManager.tryCommit(tcm).thenAccept(res -> treatClosure(res, closure));
                    break;
                case ABORT:

                case UPDATE_STATE:
                    final Timestamp<Long> commitTimestamp = stateMachineOperation.getTimestamp();
                    final Timestamp<Long> startTimestamp = stateMachineOperation.getStartTimestamp();
                    final LocalDateTime leaderTime = stateMachineOperation.getLeaderTime();
                    transactionManager.updateState(startTimestamp, commitTimestamp, leaderTime);
                    if(closure != null)
                        ((CompletableClosure<Timestamp<Long>>) closure).complete(commitTimestamp);
                    break;
                case GET_CURRENT_TIMESTAMP:
                    treatClosure(getCurrentTs(),closure);
                    break;
                case GARBAGE_COLLECTION:
                    final Timestamp<Long> newLowWaterMark = stateMachineOperation.getTimestamp();
                    transactionManager.garbageCollection(newLowWaterMark.toPrimitive());
                    break;
            }
        }
    }

    private void treatClosure(Timestamp<Long> ts, TransactionClosure closure){
        if(closure != null) {
            closure.success(ts);
            closure.run(Status.OK());
        }
    }

    @Override
    public void onLeaderStart(final long term) {
        this.transactionManager.setTerm(term);
        this.transactionManager.scheduleGarbageCollection(60 * 5, 60 * 2, TimeUnit.SECONDS, transactionManager::garbageCollection);
        this.transactionManager.triggerNonAckedFlushes();
        super.onLeaderStart(term);
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        Utils.runInThread(() -> {
            final StateSnapshot snapshot = new StateSnapshot(writer.getPath() + File.separator + "data");
            if (snapshot.save(this.transactionManager.getExtendedState())) {
                if (writer.addFile("data")) {
                    done.run(Status.OK());
                } else {
                    done.run(new Status(RaftError.EIO, "Fail to add file to writer"));
                }
            } else {
                done.run(new Status(RaftError.EIO, "Fail to save counter snapshot %s", snapshot.getPath()));
            }
        });
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            LOG.warn("Leader is not supposed to load snapshot");
            return false;
        }
        if (reader.getFileMeta("data") == null) {
            LOG.error("1 Fail to find data file in {}", reader.getPath());
            return false;
        }
        final StateSnapshot snapshot = new StateSnapshot(reader.getPath() + File.separator + "data");
        try {
            State s = snapshot.load();
            this.transactionManager.setState(s);
            return true;
        } catch (final IOException e) {
            LOG.error("2 Fail to load snapshot from {}", snapshot.getPath());
            return false;
        }
    }

    public Timestamp<Long> getCurrentTs(){
        return transactionManager.getCertifier().getCurrentCommitTs();
    }

    //Never used
    @Override
    public void onLeaderStop(final Status status) {
        this.transactionManager.setTerm(-1);
        super.onLeaderStop(status);
    }

    @Override
    public void onError(final RaftException e) {
        LOG.error("Raft error: {}", e, e);
    }

}