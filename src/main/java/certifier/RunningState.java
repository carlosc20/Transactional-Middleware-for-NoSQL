package certifier;

import java.io.Serializable;
import java.time.LocalDateTime;

public class RunningState implements Serializable {
    private int transactionsRunning;
    private LocalDateTime tombstone;

    public RunningState(){
        this.transactionsRunning = 0;
        this.tombstone = null;
    }

    public void addTransaction(){
        transactionsRunning++;
    }

    public void removeTransaction(){
        transactionsRunning--;
    }

    public void setTombstone(LocalDateTime tombstone) {
        this.tombstone = tombstone;
    }

    public LocalDateTime getTombstone() {
        return tombstone;
    }

    public boolean isCleared(){
        return transactionsRunning == 0;
    }

}
