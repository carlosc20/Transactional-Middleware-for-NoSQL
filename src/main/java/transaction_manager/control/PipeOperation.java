package transaction_manager.control;

import certifier.Timestamp;

import java.util.concurrent.CompletableFuture;

public class PipeOperation {
    Timestamp<Long> commitTimestamp;
    CompletableFuture<Void> completed;

    public PipeOperation(Timestamp<Long> commitTimestamp, CompletableFuture<Void> completed){
        this.commitTimestamp = commitTimestamp;
        this.completed = completed;
    }

    public Timestamp<Long> getCommitTimestamp() {
        return commitTimestamp;
    }

    public void setCommitTimestamp(Timestamp<Long> commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    public CompletableFuture<Void> getCompleted() {
        return completed;
    }

    public void setCompleted(CompletableFuture<Void> completed) {
        this.completed = completed;
    }
}
