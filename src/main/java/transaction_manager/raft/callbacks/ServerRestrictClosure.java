package transaction_manager.raft.callbacks;

import certifier.Timestamp;
import com.alipay.sofa.jraft.Status;

import java.util.concurrent.CompletableFuture;

public class ServerRestrictClosure<T> extends CompletableClosure<T> {

    public ServerRestrictClosure(CompletableFuture<Timestamp<Long>> completableFuture){
        setCompletableFuture(completableFuture);
    }

    @Override
    public void run(Status status) {
        //filler
    }
}
