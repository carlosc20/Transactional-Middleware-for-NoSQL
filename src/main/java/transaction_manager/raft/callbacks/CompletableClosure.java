package transaction_manager.raft.callbacks;

import com.alipay.sofa.jraft.Status;

import java.util.concurrent.CompletableFuture;

public class CompletableClosure<T> extends TransactionClosure<T> {
    CompletableFuture<Void> cf;


    public CompletableClosure(CompletableFuture<Void> completableFuture){
        cf = completableFuture;
    }

    @Override
    public void run(Status status) {
        //filler
    }
}
