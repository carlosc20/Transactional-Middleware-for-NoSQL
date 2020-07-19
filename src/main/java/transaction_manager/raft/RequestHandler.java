package transaction_manager.raft;

import certifier.Timestamp;
import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.remoting.util.StringUtils;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.StoreEngineHelper;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionContentMessage;
import transaction_manager.raft.callbacks.TransactionClosure;
import transaction_manager.raft.snapshot.ExtendedState;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.util.concurrent.Executor;

public class RequestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);
    private final RaftTMServer raftTMServer;
    private final Executor readIndexExecutor;

    public RequestHandler(RaftTMServer raftTMServer){
        this.raftTMServer = raftTMServer;
        this.readIndexExecutor = createReadIndexExecutor();
    }

    public void startTransaction(final TransactionClosure<Timestamp<Long>> closure) {
        applyOperation(TransactionManagerOperation.createStartTransaction(), closure);
    }

    public void tryCommit(TransactionContentMessage tcm, final TransactionClosure<Timestamp<Long>> closure) {
        applyOperation(TransactionManagerOperation.createCommit(tcm), closure);
    }

    //TODO any server can execute this
    public void getServersContext(final TransactionClosure<ServersContextMessage> closure){
        closure.success(getServersContext());
        closure.run(Status.OK());
    }

    private Executor createReadIndexExecutor() {
        final StoreEngineOptions opts = new StoreEngineOptions();
        return StoreEngineHelper.createReadIndexExecutor(opts.getReadIndexCoreThreads());
    }

    public void applyOperation(final TransactionManagerOperation op, final TransactionClosure<?> closure) {
        if (!isLeader()) {
            handlerNotLeaderError(closure);
            return;
        }
        try {
            closure.setTransactionManagerOperation(op);
            final Task task = new Task();
            task.setData(ByteBuffer.wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(op)));
            task.setDone(closure);
            this.raftTMServer.getNode().apply(task);
        } catch (CodecException e) {
            String errorMsg = "Fail to encode CounterOperation";
            LOG.error(errorMsg, e);
            closure.failure(errorMsg, StringUtils.EMPTY);
            closure.run(new Status(RaftError.EINTERNAL, errorMsg));
        }
    }
    private void handlerNotLeaderError(final TransactionClosure closure) {
        closure.failure("Not leader.", getRedirect());
        closure.run(new Status(RaftError.EPERM, "Not leader"));
    }

    private boolean isLeader() {
        return this.raftTMServer.getFsm().isLeader();
    }

    private String getRedirect() {
        return this.raftTMServer.redirect().getRedirect();
    }

    private ServersContextMessage getServersContext(){
        return this.raftTMServer.getFsm().getServersContext();
    }
/*
    public void getTimestamp(final boolean readOnlySafe, final CompletableClosure<Long> closure) {
        if(!readOnlySafe){
            closure.success(getTimestamp());
            closure.run(Status.OK());
            return;
        }

        this.raftTMServer.getNode().readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                if(status.isOk()){
                    closure.success(getTimestamp());
                    closure.run(Status.OK());
                    return;
                }
                CertifierServiceImpl.this.readIndexExecutor.execute(() -> {
                    if(isLeader()){
                        LOG.info("Fail to get value with 'ReadIndex': {}, try to applying to the state machine.", status);
                        applyOperation(TransactionManagerOperation.createStartTransaction(), closure);
                    }else {
                        handlerNotLeaderError(closure);
                    }
                });
            }
        });
    }

 */
}
