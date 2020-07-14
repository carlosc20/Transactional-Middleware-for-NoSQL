package jraft;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import certifier.Timestamp;
import jraft.callbacks.CompletableClosure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import jraft.snapshot.StateSnapshot;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;
import transaction_manager.utils.BitWriteSet;

import static jraft.TransactionManagerOperation.*;

/**
 * Counter state machine.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 4:52:31 PM
 */
public class StateMachine extends StateMachineAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(StateMachine.class);

    private final ExtendedState state = new ExtendedState(1000);
    /**
     * Leader term
     */
    private final AtomicLong leaderTerm = new AtomicLong(-1);

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    /**
     * Returns current timestamp.
     */
    //TODO arranjar
    public long getTimestamp() {
        try {
            return this.state.getCertifier().start().get().toPrimitive();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return -1;
    }

    //TODO
    public Timestamp<Long> getCurrentTs(){
        return this.state.getCertifier().getCurrentCommitTs();
    }

    public void onApply(final Iterator iter) {
        while (iter.hasNext()) {
            TransactionManagerOperation transactionManagerOperation = null;

            CompletableClosure<?> closure = null;
            if (iter.done() != null) {
                // This task is applied by this node, get value from closure to avoid additional parsing.
                closure = (CompletableClosure<?>) iter.done();
                transactionManagerOperation = closure.getTransactionManagerOperation();
            } else {
                // Have to parse FetchAddRequest from this user log.
                final ByteBuffer data = iter.getData();
                try {
                    transactionManagerOperation = SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(
                            data.array(), TransactionManagerOperation.class.getName());
                } catch (final CodecException e) {
                    LOG.error("Fail to decode TransactionManagerOperation", e);
                }
            }
            applyOperation(transactionManagerOperation, closure);
            iter.next();
        }
    }

    private void applyOperation(TransactionManagerOperation transactionManagerOperation, CompletableClosure<?> closure){
        if (transactionManagerOperation != null) {
            switch (transactionManagerOperation.getOp()) {
                case START_TXN:
                    state.getCertifier().start().thenAccept(ts -> resolveClosure(ts, closure));
                    //LOG.info("Get value={} at logIndex={}", current, iter.getIndex());
                    break;
                case COMMIT:
                    final BitWriteSet bws = transactionManagerOperation.getBws();
                    final Timestamp<Long> startTimestamp = transactionManagerOperation.getStartTimestamp();
                    Timestamp<Long> tc = state.getCertifier().commit(bws, startTimestamp);
                    resolveClosure(tc, closure);
                    //LOG.info("Timestamp{} at logIndex={}", current, iter.getIndex());
                    break;
                case DEL_NON_ACK_FLUSH:
                    final Timestamp<Long> txnId = transactionManagerOperation.getStartTimestamp();
                    state.removeFlush(txnId);
                    resolveClosure(null, closure);
            }
        }
    }


    private void resolveClosure(Timestamp<Long> result, CompletableClosure<?> closure){
        if (closure != null) {
             closure.getCompletableFuture().complete(result);
        }
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        //TODO colocar locks?
        Utils.runInThread(() -> {
            final StateSnapshot snapshot = new StateSnapshot(writer.getPath() + File.separator + "data");
            if (snapshot.save(this.state)) {
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
    public void onError(final RaftException e) {
        LOG.error("Raft error: {}", e, e);
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            LOG.warn("Leader is not supposed to load snapshot");
            return false;
        }
        if (reader.getFileMeta("data") == null) {
            LOG.error("Fail to find data file in {}", reader.getPath());
            return false;
        }
        final StateSnapshot snapshot = new StateSnapshot(reader.getPath() + File.separator + "data");
        try {
            ExtendedState es = snapshot.load();
            this.state.setNonAckedFlushs(es.getNonAckedFlushs());
            this.state.setCertifier(es.getCertifier());
            this.state.setLastNPVSCrash(es.getLastNPVSCrash());
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to load snapshot from {}", snapshot.getPath());
            return false;
        }

    }

    @Override
    public void onLeaderStart(final long term) {
        //TODO flush all requests
        this.leaderTerm.set(term);
        super.onLeaderStart(term);
    }

    @Override
    public void onLeaderStop(final Status status) {
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
    }

}