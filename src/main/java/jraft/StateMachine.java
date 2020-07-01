package jraft;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import certifier.CertifierImpl;
import certifier.Timestamp;
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
import jraft.snapshot.CounterSnapshotFile;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;
import transaction_manager.BitWriteSet;

import static jraft.CertifierOperation.GET_TS;
import static jraft.CertifierOperation.COMMIT;

/**
 * Counter state machine.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 4:52:31 PM
 */
public class StateMachine extends StateMachineAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(StateMachine.class);

    /**
     * Counter value
     */
    private final CertifierImpl certifier = new CertifierImpl();
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
    public Timestamp getTimestamp() {
        return this.certifier.start();
    }

    public void onApply(final Iterator iter) {
        while (iter.hasNext()) {
            Timestamp current = new Timestamp(0);
            CertifierOperation certifierOperation = null;

            CertifierClosure closure = null;
            if (iter.done() != null) {
                // This task is applied by this node, get value from closure to avoid additional parsing.
                closure = (CertifierClosure) iter.done();
                certifierOperation = closure.getCertifierOperation();
            } else {
                // Have to parse FetchAddRequest from this user log.
                final ByteBuffer data = iter.getData();
                try {
                    certifierOperation = SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(
                            data.array(), CertifierOperation.class.getName());
                } catch (final CodecException e) {
                    LOG.error("Fail to decode IncrementAndGetRequest", e);
                }
            }
            if (certifierOperation != null) {
                switch (certifierOperation.getOp()) {
                    case GET_TS:
                        current = getTimestamp();
                        LOG.info("Get value={} at logIndex={}", current, iter.getIndex());
                        break;
                    case COMMIT:
                        final BitWriteSet bws = certifierOperation.getBws();
                        final Timestamp timestamp = certifierOperation.getTimestamp();
                        current = this.certifier.commit(bws, timestamp);
                        LOG.info("Timestamp{} at logIndex={}", current, iter.getIndex());
                        break;
                }

                if (closure != null) {
                    closure.success(current);
                    closure.run(Status.OK());
                }
            }
            iter.next();
        }
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        //TODO colocar locks?
        Utils.runInThread(() -> {
            final CounterSnapshotFile snapshot = new CounterSnapshotFile(writer.getPath() + File.separator + "data");
            if (snapshot.save(this.certifier)) {
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
        final CounterSnapshotFile snapshot = new CounterSnapshotFile(reader.getPath() + File.separator + "data");
        try {
            CertifierImpl c = snapshot.load();
            this.certifier.setCurrentTs(c.getCurrentTs());
            this.certifier.setHistory(c.getHistory());
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to load snapshot from {}", snapshot.getPath());
            return false;
        }

    }

    @Override
    public void onLeaderStart(final long term) {
        this.leaderTerm.set(term);
        super.onLeaderStart(term);

    }

    @Override
    public void onLeaderStop(final Status status) {
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
    }

}