package transaction_manager.standalone;

import certifier.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.State;
import transaction_manager.TransactionManager;
import transaction_manager.TransactionManagerService;
import transaction_manager.messaging.TransactionContentMessage;

import java.util.concurrent.CompletableFuture;

public class StandaloneTMService extends TransactionManagerService implements TransactionManager {
    private static final Logger LOG = LoggerFactory.getLogger(StandaloneTMService.class);
    private final State state;

    public StandaloneTMService(int npvsStubPort, int npvsPort, String databaseURI, String databaseName, String databaseCollectionName, long timestep){
        super(timestep, npvsStubPort, npvsPort, databaseURI, databaseName, databaseCollectionName);
        state = new State(timestep);
    }

    @Override
    public CompletableFuture<Timestamp<Long>> startTransaction(){
        return state.getCertifier().start();
    }

    @Override
    public CompletableFuture<Boolean> tryCommit(TransactionContentMessage tc) {
        Timestamp<Long> commitTimestamp = state.getCertifier().commit(tc.getWriteSet(), tc.getStartTimestamp());
        if(commitTimestamp.toPrimitive() > 0) {
            //TODO muito importante e se falha?
            //TODO resposta do flush vir desordenada
            //TODO return correto
            LOG.info("Making transaction with TC: {} changes persist", commitTimestamp.toPrimitive());
            return flush(tc, commitTimestamp, state.getCertifier().getCurrentCommitTs())
                    .thenApply(x -> true);
        } else {
            LOG.info("aborted a transaction with TS {}", tc.getStartTimestamp());
            return CompletableFuture.completedFuture(false);
        }
    }

    @Override
    public void updateState(Timestamp<Long> commitTimestamp) {
        state.getCertifier().update(commitTimestamp);
    }
}
