package transaction_manager;

import certifier.Certifier;
import certifier.CertifierImpl;
import certifier.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.messaging.TransactionContentMessage;

import java.util.concurrent.CompletableFuture;

public class StandaloneTMService extends TransactionManagerService{
    private static final Logger LOG = LoggerFactory.getLogger(StandaloneTMService.class);
    private final Certifier<Long> certifier;

    public StandaloneTMService(int npvsStubPort, int npvsPort, String databaseURI, String databaseName, String databaseCollectionName, long timestep){
        super(npvsStubPort, npvsPort, databaseURI, databaseName, databaseCollectionName);
        certifier = new CertifierImpl(timestep);
    }

    @Override
    public Timestamp<Long> startTransaction(){
        return certifier.start();
    }

    @Override
    public CompletableFuture<Boolean> tryCommit(TransactionContentMessage tc) {
        Timestamp<Long> commitTimestamp = certifier.commit(tc.getWriteSet(), tc.getStartTimestamp());
        if(commitTimestamp.toPrimitive() > 0) {
            //TODO muito importante e se falha?
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
}
