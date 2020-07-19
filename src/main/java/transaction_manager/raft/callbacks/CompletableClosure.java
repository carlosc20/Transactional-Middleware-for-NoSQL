package transaction_manager.raft.callbacks;

import certifier.Timestamp;
import com.alipay.sofa.jraft.Status;

import java.sql.Time;
import java.util.concurrent.CompletableFuture;

public class CompletableClosure<T> extends TransactionClosure<T> {
    CompletableFuture<Timestamp<Long>> cf;


    public CompletableClosure(CompletableFuture<Timestamp<Long>> completableFuture){
        cf = completableFuture;
    }

    public void complete(Timestamp<Long> ts){
        cf.complete(ts);
    }

    @Override
    public void run(Status status) {
        //filler
    }
}
