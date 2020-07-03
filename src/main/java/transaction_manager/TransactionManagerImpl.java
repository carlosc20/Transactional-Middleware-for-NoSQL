package transaction_manager;

import certifier.Certifier;
import certifier.CertifierImpl;

import certifier.Timestamp;
import nosql.KeyValueDriver;
import nosql.MongoKV;
import npvs.NPVS;
import npvs.NPVSStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.messaging.TransactionContentMessage;
import transaction_manager.utils.KeyValue;
import utils.ByteArrayWrapper;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class TransactionManagerImpl implements TransactionManager {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionManagerImpl.class);

    private final ExecutorService taskExecutor;
    private final KeyValueDriver driver;
    private final Certifier<Long> certifier;
    private final NPVS<Long> npvs;
    private final ServersContextMessage scm;

    public TransactionManagerImpl(int myPort, int npvsPort, String databaseURI, String databaseName, String databaseCollectionName){
        taskExecutor = Executors.newFixedThreadPool(8);
        npvs = new NPVSStub(myPort, npvsPort);
        driver = new MongoKV(databaseURI, databaseName, databaseCollectionName);
        certifier = new CertifierImpl(10000);
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
            return flush(tc, commitTimestamp, certifier.getCurrentCommitTs())
                .thenApply(x -> {
                    certifier.update();
                    return true;
                });
        } else {
            LOG.debug("aborted a tx with startTimestamp {}", tc.getStartTimestamp());
            return CompletableFuture.completedFuture(false);
        }
    }

    //TODO fix
    private CompletableFuture<Void> flush(TransactionContentMessage tc, Timestamp<Long> provisionalCommitTimestamp, Timestamp<Long> currentCommitTimestamp) {
        Map<ByteArrayWrapper, byte[]> writeMap = tc.getWriteMap();
        List<CompletableFuture<KeyValue>> keyValues = writeMap.keySet()
            .stream()
            .map(key -> driver.getWithoutTS(key)
                .thenApply(value -> new KeyValue(key, value)))
            .collect(Collectors.toList());

        return CompletableFuture.allOf(keyValues.toArray(new CompletableFuture[0]))
            .thenApply(future -> keyValues.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList()))
            .thenApply(future -> future.stream()
                .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue)))
            .thenComposeAsync(future -> npvs.put(future, currentCommitTimestamp), taskExecutor)
            .thenComposeAsync(future -> driver.put(writeMap, provisionalCommitTimestamp), taskExecutor);
    }

    public ServersContextMessage getServersContext(){
        return scm;
    }
}
