package transaction_manager;

import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import nosql.KeyValueDriver;
import npvs.NPVS;
import npvs.messaging.FlushMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.raft.FlushAgainInfo;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public abstract class BatchTransactionManagerService extends TransactionManagerService{
    private static final Logger LOG = LoggerFactory.getLogger(BatchTransactionManagerService.class);
    private final Timestamp<Long> commitUpperLimit;
    private final Timestamp<Long> startUpperLimit;
    private final Map<ByteArrayWrapper, byte[]> writeMap;
    private final List<CompletableFuture<Timestamp<Long>>> completableFutures;
    private boolean waitingForBatch;
    private final int batchTimeout;

    public BatchTransactionManagerService(int batchTimeout, NPVS<Long> npvs, KeyValueDriver driver, ServersContextMessage scm){
        super(npvs, driver, scm);
        this.commitUpperLimit = new MonotonicTimestamp(0);
        this.startUpperLimit = new MonotonicTimestamp(0);
        this.writeMap = new HashMap<>();
        this.completableFutures = new ArrayList<>();
        this.waitingForBatch = false;
        this.batchTimeout = batchTimeout;
    }

    public CompletableFuture<Timestamp<Long>> updateState(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> transactionStartTimestamp,
                            Timestamp<Long> commitTimestamp){

        CompletableFuture<Timestamp<Long>> res = new CompletableFuture<>();
        completableFutures.add(res);
        commitUpperLimit.set(commitTimestamp);
        startUpperLimit.set(transactionStartTimestamp);
        this.writeMap.putAll(writeMap);
        return res;
    }

    public void clearState(){
        this.writeMap.clear();
        this.completableFutures.clear();
        waitingForBatch = false;
    }

    public CompletableFuture<Timestamp<Long>> flushInBatch(Map<ByteArrayWrapper, byte[]> writeMap,
                   Timestamp<Long> transactionStartTimestamp, Timestamp<Long> commitTimestamp, Timestamp<Long> currentTimestamp){

        CompletableFuture<Timestamp<Long>> res = updateState(writeMap, commitTimestamp, transactionStartTimestamp);
        if(!waitingForBatch){
            waitingForBatch = true;
            this.executorService.schedule(()->{
                LOG.info("Building batch with size={}", completableFutures.size());
                Map<ByteArrayWrapper, byte[]> map = new HashMap<>(this.writeMap);
                List<CompletableFuture<Timestamp<Long>>> cfs = new ArrayList<>(this.completableFutures);
                clearState();
                FlushMessage flushMessage = new FlushMessage(map, startUpperLimit, currentTimestamp);
                setNonAckedFlush(new FlushAgainInfo(flushMessage, commitTimestamp));
                getCommitControlHandler().putBatch(commitTimestamp);
                flush(flushMessage, commitTimestamp, cfs);
            }, batchTimeout, TimeUnit.MILLISECONDS);
        }
        return res;
    }
}
