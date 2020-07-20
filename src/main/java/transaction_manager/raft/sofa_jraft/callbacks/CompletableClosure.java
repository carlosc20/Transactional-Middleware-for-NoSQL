package transaction_manager.raft.sofa_jraft.callbacks;

import certifier.Timestamp;
import com.alipay.sofa.jraft.Status;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CompletableClosure<T> extends TransactionClosure<T> {
    List<CompletableFuture<Timestamp<Long>>> cfs;


    public CompletableClosure(List<CompletableFuture<Timestamp<Long>>> completableFuture){
        cfs = completableFuture;
    }

    public void complete(Timestamp<Long> ts){
        cfs.forEach(cf -> cf.complete(ts));
    }

    @Override
    public void run(Status status) {
        //filler
    }
}
