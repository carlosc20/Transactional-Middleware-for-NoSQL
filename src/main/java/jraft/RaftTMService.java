package jraft;

import certifier.Timestamp;
import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.remoting.util.StringUtils;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.StoreEngineHelper;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import jraft.callbacks.CompletableClosure;
import jraft.callbacks.ServerRestrictClosure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.TransactionManagerService;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionContentMessage;
import transaction_manager.utils.ByteArrayWrapper;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class RaftTMService extends TransactionManagerService {
    private static final Logger LOG = LoggerFactory.getLogger(RaftTMService.class);

    private final RaftTMServer raftTMServer;
    private final Executor readIndexExecutor;


    public RaftTMService(RaftTMServer raftTMServer, int npvsStubPort, int npvsPort, String databaseURI, String databaseName, String databaseCollectionName) {
        super(npvsStubPort, npvsPort, databaseURI, databaseName, databaseCollectionName);
        this.raftTMServer = raftTMServer;
        this.readIndexExecutor = createReadIndexExecutor();
    }

    public void startTransaction(final CompletableClosure<Long> closure) {
        closure.setCompletableFuture(new CompletableFuture<>());
        applyOperation(TransactionManagerOperation.createStartTransaction(), closure);
    }

    public void tryCommit(TransactionContentMessage tx, final CompletableClosure<Boolean> closure) {
        final Map<ByteArrayWrapper, byte[]> writeMap = tx.getWriteMap();
        final Timestamp<Long> startTimestamp = tx.getStartTimestamp();
        CompletableFuture<Timestamp<Long>> cf = new CompletableFuture<>();
        cf.thenAccept(tc -> {
            if (tc.toPrimitive() > 0){
                CompletableFuture<Map<ByteArrayWrapper, byte[]>> consistentKeyValues = getPreviousConsistentValues(writeMap);
                //TODO verificar o getCurrentTimestamp()
                consistentKeyValues.thenCompose(wm -> saveToNPVS(wm, getCurrentTimestamp()))
                    .thenCompose(future -> saveToDB(writeMap, tc))
                    .thenAccept(x -> {
                        closure.success(true);
                        closure.run(Status.OK());
                    })
                    .thenComposeAsync(x -> deleteNonAckedFlush(startTimestamp));
            }
            else{
                closure.success(false);
                closure.run(Status.OK());
            }
        });
        closure.setCompletableFuture(cf);
        applyOperation(TransactionManagerOperation.createCommit(startTimestamp, writeMap, tx.getWriteSet()), closure);
    }

    public CompletableFuture<Void> deleteNonAckedFlush(Timestamp<Long> startTimestamp){
        CompletableFuture<Void> cf = new CompletableFuture<>();
        CompletableClosure<Void> cc = new ServerRestrictClosure<>(cf);
        applyOperation(TransactionManagerOperation.createDeleteNonAckFlush(startTimestamp), cc);
        return cf;
    }


    public void getServersContext(final CompletableClosure<ServersContextMessage> closure){
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

    private void applyOperation(final TransactionManagerOperation op, final CompletableClosure<?> closure) {
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

    private void handlerNotLeaderError(final CompletableClosure closure) {
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
