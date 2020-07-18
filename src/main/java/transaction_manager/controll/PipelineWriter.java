package transaction_manager.controll;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

public class PipelineWriter {
    private final ExecutorService e;
    private final Queue<CompletableFuture<Void>> nextToPutInPipe;
    private boolean inconsistent;
    private ReentrantLock rl;

    public PipelineWriter(ExecutorService e){
        this.e = e;
        this.inconsistent = false;
        this.rl = new ReentrantLock();
        this.nextToPutInPipe = new LinkedList<>();
    }

    public CompletableFuture<Void> put(long id, CompletableFuture<Void> timestamp, CompletableFuture<Void> writeValues){
        if (nextToPutInPipe.size() == 0 && !inconsistent){
            System.out.println("11111");
            return write(id, timestamp, writeValues);
        }
        else{
            System.out.println("222222");
            CompletableFuture<Void> new_cf = new CompletableFuture<>();
            nextToPutInPipe.add(new_cf);
            return new_cf.thenCompose(x -> write(id, timestamp, writeValues));
        }
    }

    private CompletableFuture<Void> write(long id, CompletableFuture<Void> timestamp, CompletableFuture<Void> write){
        inconsistent = true;
        return timestamp.thenAccept(x ->  System.out.println("writing timestamp from: " + id))
                .thenComposeAsync(x -> {
                    System.out.println("333333");
                    inconsistent = false;
                    CompletableFuture.runAsync(() -> {
                        System.out.println("44444");
                        if(nextToPutInPipe.size() > 0)
                            nextToPutInPipe.poll().complete(null);
                    }, e);
                return write.thenAccept(y -> System.out.println("writing values from id: " + id));
            });
    }

    /*
    private CompletableFuture<Void> write(int id, CompletableFuture<Void> timestamp, CompletableFuture<Void> write){
        inconsistent = true;
        return timestamp.thenAccept(x ->  System.out.println("writing timestamp from: " + id)).thenComposeAsync(x -> {
            CompletableFuture.runAsync(() -> {
                inconsistent = false;
                if(nextToPutInPipe.size() > 0)
                    nextToPutInPipe.poll().complete(null);
            }, e);
            return write.thenAccept(y -> System.out.println("writing values from id: " + id));
        }, e);
    }

     */
}
