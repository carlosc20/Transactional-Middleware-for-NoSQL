package jraft.callbacks;

import com.alipay.sofa.jraft.Status;

import java.util.concurrent.CompletableFuture;

public class ServerRestrictClosure<V,T> extends CompletableClosure<V,T> {

    public ServerRestrictClosure(CompletableFuture<T> completableFuture){
        setCompletableFuture(completableFuture);
    }

    @Override
    public void run(Status status) {
        //filler
    }
}
