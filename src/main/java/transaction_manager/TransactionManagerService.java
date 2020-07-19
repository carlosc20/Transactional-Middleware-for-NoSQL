package transaction_manager;
import certifier.Timestamp;

import nosql.KeyValueDriver;
import npvs.NPVS;
import npvs.messaging.FlushMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.control.CommitControlHandler;
import transaction_manager.control.FlushControlHandler;
import transaction_manager.control.PipelineWriterHandler;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.control.CommitOrderHandler;
import transaction_manager.utils.ByteArrayWrapper;
import transaction_manager.utils.KeyValue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public abstract class TransactionManagerService {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionManagerService.class);
    private final KeyValueDriver driver;
    //stub
    private final ScheduledExecutorService executorService;
    private final NPVS<Long> npvs;
    private final ServersContextMessage scm;
    private final CommitControlHandler commitControlHandler;
    private final FlushControlHandler flushControlHandler;

    public TransactionManagerService(long timestep, NPVS<Long> npvs, KeyValueDriver driver, ServersContextMessage scm) {
        this.executorService = Executors.newScheduledThreadPool(8);
        this.npvs = npvs;
        this.driver = driver;
        this.scm = scm;
        this.commitControlHandler = new CommitOrderHandler(timestep);
        this.flushControlHandler = new PipelineWriterHandler(executorService);
    }

    public abstract void updateState(Timestamp<Long> startTimestamp, Timestamp<Long> commitTimestamp, CompletableFuture<Timestamp<Long>> cf);

    public CompletableFuture<Timestamp<Long>> flush(FlushMessage flushMessage, Timestamp<Long> provisionalCommitTimestamp) {
        Map<ByteArrayWrapper, byte[]> writeMap = flushMessage.getWriteMap();
        CompletableFuture<Map<ByteArrayWrapper, byte[]>> consistentKeyValues = getPreviousConsistentValues(writeMap);
        CompletableFuture<Timestamp<Long>> cf = new CompletableFuture<>();

        consistentKeyValues.thenCompose(wm -> saveToNPVS(flushMessage))
            .thenCompose(x -> saveToDB(writeMap, provisionalCommitTimestamp))
            .thenCompose(x -> commitControlHandler.deliver(provisionalCommitTimestamp))
            .thenAccept(x -> updateState(flushMessage.getTransactionStartTimestamp(), provisionalCommitTimestamp, cf))
            .thenAccept(x -> commitControlHandler.completeDeliveries());

        return cf;
    }

    public CompletableFuture<Map<ByteArrayWrapper, byte[]>> getPreviousConsistentValues(Map<ByteArrayWrapper, byte[]> writeMap){
        LOG.info("Fetching consistent key/values that belong to the commiting transaction from the DB");
        List<CompletableFuture<KeyValue>> keyValues =  writeMap.keySet()
                .stream()
                .map(key -> driver.getWithoutTS(key).thenApply(value -> new KeyValue(key, value)))
                .collect(Collectors.toList());

        return CompletableFuture.allOf(keyValues.toArray(new CompletableFuture[0]))
                .thenApply(future -> keyValues.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList()))
                .thenApply(future -> future.stream()
                    .filter(KeyValue::valueNotNull)
                    .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue)));
    }

    public CompletableFuture<Void> saveToNPVS(FlushMessage flushMessage){
        if(flushMessage.getWriteMap().size() > 0){
            LOG.info("Putting consistent key/values in NPVS with TC: {}", flushMessage.getCurrentTimestamp().toPrimitive());
            return npvs.put(flushMessage);
        }
        LOG.info("No old consistent key/values to be transfered to TC: {}", flushMessage.getCurrentTimestamp().toPrimitive());
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> saveToDB(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> provisionalCommitTimestamp){
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOG.info("Putting new key/values in the DB with TC: {}", provisionalCommitTimestamp.toPrimitive());
        return flushControlHandler.put(driver.put(provisionalCommitTimestamp), driver.put(writeMap));
    }

    public ServersContextMessage getServersContext() {
        return scm;
    }

    public ScheduledExecutorService getExecutorService() {
        return executorService;
    }

    public CommitControlHandler getCommitControlHandler() {
        return commitControlHandler;
    }

    public FlushControlHandler getFlushControlHandler() {
        return flushControlHandler;
    }
}
