package transaction_manager.control;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PipelineWriterHandler implements FlushControlHandler {
    private final ExecutorService e;
    private final ExecutorService singleExecutor;
    private final Queue<CompletableFuture<Void>> nextToPutInPipe;
    private boolean inconsistent;

    public PipelineWriterHandler(ExecutorService e){
        this.e = e;
        this.inconsistent = false;
        this.singleExecutor = Executors.newSingleThreadExecutor();
        this.nextToPutInPipe = new LinkedList<>();
    }

    public CompletableFuture<Void> put(CompletableFuture<Void> timestamp, CompletableFuture<Void> writeValues){
        CompletableFuture<Void> res = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            if (nextToPutInPipe.size() == 0 && !inconsistent){
                write(timestamp, writeValues, res);
            }
            else{
                CompletableFuture<Void> new_cf = new CompletableFuture<>();
                nextToPutInPipe.add(new_cf);
                new_cf.thenAcceptAsync(x -> write(timestamp, writeValues, res), singleExecutor);
            }
        }, singleExecutor);
        return res;
    }

    private void write(CompletableFuture<Void> timestamp, CompletableFuture<Void> write, CompletableFuture<Void> res){
        inconsistent = true;
        timestamp.thenAccept(x -> {
            inconsistent = false;
            CompletableFuture.runAsync(() -> {
                if(nextToPutInPipe.size() > 0) {
                    CompletableFuture<Void> cf = nextToPutInPipe.poll();
                    cf.complete(null);
                }
            }, e);
            CompletableFuture.runAsync(()-> write.thenAcceptAsync(y -> res.complete(null),e) ,e);
        });
    }
}
