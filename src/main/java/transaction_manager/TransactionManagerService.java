package transaction_manager;
import certifier.Timestamp;

import nosql.KeyValueDriver;
import nosql.MongoAsynchKV;
import npvs.NPVS;
import npvs.NPVSStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionContentMessage;
import transaction_manager.ordering.CommitOrderDeliveryHandler;
import transaction_manager.utils.ByteArrayWrapper;
import transaction_manager.utils.KeyValue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public abstract class TransactionManagerService {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionManagerService.class);
    private final KeyValueDriver driver;
    private final NPVS<Long> npvs;
    private final ServersContextMessage scm;
    private final CommitOrderDeliveryHandler codh;

    public TransactionManagerService(long timestep, int npvsStubPort, int npvsPort, String databaseURI, String databaseName, String databaseCollectionName) {
        npvs = new NPVSStub(npvsStubPort, npvsPort);
        driver = new MongoAsynchKV(databaseURI, databaseName, databaseCollectionName);
        scm = new ServersContextMessage(databaseURI, databaseName, databaseCollectionName, npvsPort);
        codh = new CommitOrderDeliveryHandler(timestep);
    }

    public abstract void updateState();

    public CompletableFuture<Void> flush(TransactionContentMessage tc, Timestamp<Long> provisionalCommitTimestamp, Timestamp<Long> currentCommitTimestamp) {
        Map<ByteArrayWrapper, byte[]> writeMap = tc.getWriteMap();
        CompletableFuture<Map<ByteArrayWrapper, byte[]>> consistentKeyValues = getPreviousConsistentValues(writeMap);
        return consistentKeyValues.thenCompose(wm -> saveToNPVS(wm, currentCommitTimestamp))
                .thenCompose(future -> saveToDB(writeMap, provisionalCommitTimestamp))
                .thenCompose(future -> codh.returnInOrder(provisionalCommitTimestamp))
                .thenAccept(x -> updateState())
                .thenAccept(x -> codh.completeNewInOrder());
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

    public CompletableFuture<Void> saveToNPVS(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> currentCommitTimestamp){
        if(writeMap.size() > 0){
            LOG.info("Putting consistent key/values in NPVS with TC: {}", currentCommitTimestamp.toPrimitive());
            return npvs.put(writeMap, currentCommitTimestamp);
        }
        LOG.info("No old consistent key/values to be transfered to TC: {}", currentCommitTimestamp.toPrimitive());
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> saveToDB(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> provisionalCommitTimestamp){
        LOG.info("Putting new key/values in the DB with TC: {}", provisionalCommitTimestamp.toPrimitive());
        return driver.put(writeMap, provisionalCommitTimestamp);
    }

    public ServersContextMessage getServersContext() {
        return scm;
    }
}
