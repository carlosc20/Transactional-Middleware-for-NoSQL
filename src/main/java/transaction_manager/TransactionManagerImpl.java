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

    public TransactionManagerImpl(){
        taskExecutor = Executors.newFixedThreadPool(8);
        npvs = new NPVSStub(0,0);
        driver = new MongoKV("mongodb://127.0.0.1:27017", "lei", "teste");
        certifier = new CertifierImpl(10000);
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

    //De momento não considera qualquer tipo de erro nos pedidos.
    // versão em que tanto espera pelo flush da bd como NPVS
    //TODO se não considerarmos erros podemos er uma versão que apenas espera pelo npvs e lemos sempre de lá, para comparar
    private CompletableFuture<Void> flush(TransactionContentMessage tc, Timestamp<Long> provisionalCommitTimestamp, Timestamp<Long> currentCommitTimestamp) {
        Map<ByteArrayWrapper, byte[]> writeMap = tc.getWriteMap();
        List<CompletableFuture<KeyValue>> keyValues = writeMap.keySet()
            .stream()
            .map(key -> driver.get(key.getData())
                .thenApply(value -> new KeyValue(key, value)))
            .collect(Collectors.toList());

        return CompletableFuture.allOf(keyValues.toArray(new CompletableFuture[0]))
            .thenApply(future -> keyValues.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList()))
            .thenApply(future -> future.stream()
                .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue)))
            .thenComposeAsync(future -> npvs.put(future, currentCommitTimestamp), taskExecutor)
            .thenComposeAsync(future -> CompletableFuture.allOf(driver.put(writeMap), npvs.put(writeMap, provisionalCommitTimestamp)), taskExecutor);
    }
}
