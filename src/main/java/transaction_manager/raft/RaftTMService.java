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
import transaction_manager.TransactionManagerService;
import transaction_manager.raft.callbacks.TransactionClosure;
import transaction_manager.raft.callbacks.CompletableClosure;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionContentMessage;
import transaction_manager.utils.ByteArrayWrapper;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class RaftTMService {
    private static final Logger LOG = LoggerFactory.getLogger(RaftTMService.class);
/*
    private final RaftTMServer raftTMServer;
    private final Executor readIndexExecutor;

    public RaftTMService(long timestep, RaftTMServer raftTMServer, int npvsStubPort, int npvsPort, String databaseURI, String databaseName, String databaseCollectionName) {
        super(timestep, npvsStubPort, npvsPort, databaseURI, databaseName, databaseCollectionName);
        this.raftTMServer = raftTMServer;
        this.readIndexExecutor = createReadIndexExecutor();
    }

    public void startTransaction(final TransactionClosure<Long> closure) {
        closure.setCompletableFuture(new CompletableFuture<>());
        applyOperation(TransactionManagerOperation.createStartTransaction(), closure);
    }

    public void tryCommit(TransactionContentMessage tx, final TransactionClosure<Boolean> closure) {
        CompletableFuture<Timestamp<Long>> cf = new CompletableFuture<>();
        cf.thenAccept(commitTimestamp -> commitConsumer(commitTimestamp, closure, tx.getWriteMap()));
        closure.setCompletableFuture(cf);
        applyOperation(TransactionManagerOperation.createCommit(tx.getTimestamp(), tx.getWriteMap(), tx.getWriteSet()), closure);
    }

    private void commitConsumer(Timestamp<Long> commitTimestamp, TransactionClosure<Boolean> closure, Map<ByteArrayWrapper, byte[]> writeMap){
        if (commitTimestamp.toPrimitive() > 0){
            CompletableFuture<Map<ByteArrayWrapper, byte[]>> consistentKeyValues = getPreviousConsistentValues(writeMap);
            //TODO verificar o getCurrentTimestamp()
            consistentKeyValues.thenCompose(wm -> saveToNPVS(wm, getCurrentTimestamp()))
                .thenCompose(future -> saveToDB(writeMap, commitTimestamp))
                .thenAccept(x -> {
                    closure.success(true);
                    closure.run(Status.OK());})
                .thenAccept(x -> updateState(commitTimestamp));
        }
        else{
            closure.success(false);
            closure.run(Status.OK());
        }
    }

    @Override
    public void updateState(Timestamp<Long> commitTimestamp) {
        CompletableFuture<Timestamp<Long>> cf = new CompletableFuture<>();
        TransactionClosure<Void> cc = new CompletableClosure<>(cf);
        applyOperation(TransactionManagerOperation.createUpdateState(commitTimestamp), cc);
    }


    public void getServersContext(final TransactionClosure<ServersContextMessage> closure){
        if (!isLeader())
            handlerNotLeaderError(closure);
        else{
            closure.success(getServersContext());
            closure.run(Status.OK());
        }
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

    private boolean isLeader() {
        return this.raftTMServer.getFsm().isLeader();
    }

    private long getTimestamp() {
        return this.raftTMServer.getFsm().getTimestamp();
    }

    private Timestamp<Long> getCurrentTimestamp(){
        return this.raftTMServer.getFsm().getCurrentTs();
    }

    private String getRedirect() {
        return this.raftTMServer.redirect().getRedirect();
    }

    private void handlerNotLeaderError(final TransactionClosure closure) {
        closure.failure("Not leader.", getRedirect());
        closure.run(new Status(RaftError.EPERM, "Not leader"));
    }
/*
@Override
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
