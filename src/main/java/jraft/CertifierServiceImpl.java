package jraft;

/**
 * @author likun (saimu.msm@antfin.com)
 */

public class CertifierServiceImpl implements CertifierService {
    /*
    private static final Logger LOG = LoggerFactory.getLogger(CertifierServiceImpl.class);

    private final CertifierServer certifierServer;
    private final Executor      readIndexExecutor;

    public CertifierServiceImpl(CertifierServer certifierServer) {
        this.certifierServer = certifierServer;
        this.readIndexExecutor = createReadIndexExecutor();
    }

    private Executor createReadIndexExecutor() {
        final StoreEngineOptions opts = new StoreEngineOptions();
        return StoreEngineHelper.createReadIndexExecutor(opts.getReadIndexCoreThreads());
    }

    @Override
    public void getTimestamp(final boolean readOnlySafe, final CertifierClosure<Long> closure) {
        if(!readOnlySafe){
            closure.success(getTimestamp());
            closure.run(Status.OK());
            return;
        }

        this.certifierServer.getNode().readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                if(status.isOk()){
                    closure.success(getTimestamp());
                    closure.run(Status.OK());
                    return;
                }
                CertifierServiceImpl.this.readIndexExecutor.execute(() -> {
                    if(isLeader()){
                        LOG.debug("Fail to get value with 'ReadIndex': {}, try to applying to the state machine.", status);
                        applyOperation(CertifierOperation.createGetTimestamp(), closure);
                    }else {
                        handlerNotLeaderError(closure);
                    }
                });
            }
        });
    }

    private boolean isLeader() {
        return this.certifierServer.getFsm().isLeader();
    }

    private long getTimestamp() {
        return this.certifierServer.getFsm().getTimestamp();
    }

    private String getRedirect() {
        return this.certifierServer.redirect().getRedirect();
    }

    @Override
    public void commit(final BitWriteSet bws, final MonotonicTimestamp monotonicTimestamp, final CertifierClosure<Long> closure) {
        applyOperation(CertifierOperation.createCommit(bws, monotonicTimestamp), closure);
    }


    private void applyOperation(final CertifierOperation op, final CertifierClosure<Long> closure) {
        if (!isLeader()) {
            handlerNotLeaderError(closure);
            return;
        }
        try {
            closure.setCertifierOperation(op);
            final Task task = new Task();
            task.setData(ByteBuffer.wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(op)));
            task.setDone(closure);
            this.certifierServer.getNode().apply(task);
        } catch (CodecException e) {
            String errorMsg = "Fail to encode CounterOperation";
            LOG.error(errorMsg, e);
            closure.failure(errorMsg, StringUtils.EMPTY);
            closure.run(new Status(RaftError.EINTERNAL, errorMsg));
        }
    }

    private void handlerNotLeaderError(final CertifierClosure closure) {
        closure.failure("Not leader.", getRedirect());
        closure.run(new Status(RaftError.EPERM, "Not leader"));
    }

     */
}

