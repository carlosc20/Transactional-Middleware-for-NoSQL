package transaction_manager.control;

import certifier.Timestamp;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PipelineWriterHandler implements FlushControlHandler {
    private final ExecutorService e;
    private final ExecutorService singleExecutor;
    private List<Timestamp<Long>> pipeOrder;
    private final Queue<PipeOperation> nextToPutInPipe;

    public PipelineWriterHandler(ExecutorService e){
        this.e = e;
        this.singleExecutor = Executors.newSingleThreadExecutor();
        this.pipeOrder = new ArrayList<>();
        this.nextToPutInPipe = new LinkedList<>();
    }

    public CompletableFuture<Void> put(Timestamp<Long> commitTimestamp, CompletableFuture<Boolean> timestamp, CompletableFuture<Boolean> writeValues){
        CompletableFuture<Void> res = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            if (pipeOrder.get(0).equals(commitTimestamp)){
                pipeOrder.remove(0);
                write(timestamp, writeValues, res);
            }
            else{
                CompletableFuture<Void> new_cf = new CompletableFuture<>();
                nextToPutInPipe.add(new PipeOperation(commitTimestamp, new_cf));
                new_cf.thenAcceptAsync(x -> write(timestamp, writeValues, res), singleExecutor);
            }
        }, singleExecutor);
        return res;
    }

    private void write(CompletableFuture<Boolean> timestamp, CompletableFuture<Boolean> write, CompletableFuture<Void> res){
        timestamp.thenAccept(okTs -> {
            if(okTs) {
                CompletableFuture.runAsync(() -> {
                    if (nextToPutInPipe.size() > 0)
                        completeNextPipe();
                }, e);
                CompletableFuture.runAsync(() -> callWriteValues(write, res), e);
            }
            else{
                write(timestamp, write, res);
            }
        });
    }

    private void completeNextPipe(){
        nextToPutInPipe.forEach(v -> {
            if (v.commitTimestamp.equals(pipeOrder.get(0))){
                v.completed.complete(null);
            }
        });
    }

    private void callWriteValues(CompletableFuture<Boolean> write, CompletableFuture<Void> res){
        write.thenAccept(okWrite -> {
            if (okWrite)
                res.complete(null);
            else
                callWriteValues(write, res);
        });
    }

    public void putPipe(Timestamp<Long> timestamp){
        this.pipeOrder.add(timestamp);
    }
}
