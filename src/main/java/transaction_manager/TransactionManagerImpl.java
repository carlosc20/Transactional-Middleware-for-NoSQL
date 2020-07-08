package transaction_manager;

import certifier.Certifier;
import certifier.CertifierImpl;

import certifier.Timestamp;
import nosql.KeyValueDriver;
import nosql.MongoAsynchKV;
import nosql.MongoKV;
import npvs.NPVS;
import npvs.NPVSStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionContentMessage;
import transaction_manager.utils.KeyValue;
import transaction_manager.utils.ByteArrayWrapper;
import utils.Timer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class TransactionManagerImpl implements TransactionManager {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionManagerImpl.class);
    private static final Timer TIMER = new Timer();

    private final ExecutorService taskExecutor;
    private final KeyValueDriver driver;
    private final Certifier<Long> certifier;
    private final NPVS<Long> npvs;
    private final ServersContextMessage scm;

    public TransactionManagerImpl(int npvsStubPort, int npvsPort, String databaseURI, String databaseName, String databaseCollectionName){
        taskExecutor = Executors.newFixedThreadPool(8);
        npvs = new NPVSStub(npvsStubPort, npvsPort);
        driver = new MongoAsynchKV(databaseURI, databaseName, databaseCollectionName);
        certifier = new CertifierImpl(1000);
        this.scm = new ServersContextMessage(databaseURI, databaseName, databaseCollectionName, npvsPort);
    }

    public Timestamp<Long> startTransaction(){
        return certifier.start();
    }

    @Override
    public CompletableFuture<Boolean> tryCommit(TransactionContentMessage tc) {
        Timestamp<Long> commitTimestamp = certifier.commit(tc.getWriteSet(), tc.getStartTimestamp());
        if(commitTimestamp.toPrimitive() > 0) {
            //TODO e se falha?
            //TODO return correto
            LOG.info("Making transaction with TC: {} changes persist", commitTimestamp.toPrimitive());
            return flush(tc, commitTimestamp, certifier.getCurrentCommitTs())
                .thenApply(x -> {
                    certifier.update();
                    return true;
                });
        } else {
            LOG.info("aborted a transaction with TS {}", tc.getStartTimestamp());
            return CompletableFuture.completedFuture(false);
        }
    }

    private CompletableFuture<Void> flush(TransactionContentMessage tc, Timestamp<Long> provisionalCommitTimestamp, Timestamp<Long> currentCommitTimestamp) {
        Map<ByteArrayWrapper, byte[]> writeMap = tc.getWriteMap();
        LOG.info("Fetching consistent key/values that belong to the commiting transaction from the DB");
        List<CompletableFuture<KeyValue>> keyValues = writeMap.keySet()
            .stream()
            .map(key -> driver.getWithoutTS(key).thenApply(value -> new KeyValue(key, value)))
            .collect(Collectors.toList());

        return CompletableFuture.allOf(keyValues.toArray(new CompletableFuture[0]))
            .thenApply(future -> keyValues.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()))
            .thenApply(future -> future.stream()
                .filter(KeyValue::valueNotNull)
                .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue)))
            .thenComposeAsync(wm -> {
                if(wm.size() > 0){
                    LOG.info("Putting consistent key/values in NPVS with TC: {}", currentCommitTimestamp.toPrimitive());
                    return npvs.put(wm, currentCommitTimestamp);
                }
                LOG.info("No old consistent key/values to be transfered to TC: {}", currentCommitTimestamp.toPrimitive());
                return CompletableFuture.completedFuture(null);
            }, taskExecutor)
            .thenComposeAsync(future -> {
                LOG.info("Putting new key/values in the DB with TC: {}", provisionalCommitTimestamp.toPrimitive());
                return driver.put(writeMap, provisionalCommitTimestamp);
            }, taskExecutor);
    }

    public ServersContextMessage getServersContext(){
        return scm;
    }
}
