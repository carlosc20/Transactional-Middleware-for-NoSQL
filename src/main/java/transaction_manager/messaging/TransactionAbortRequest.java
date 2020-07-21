package transaction_manager.messaging;

import certifier.Timestamp;

import java.io.Serializable;

public class TransactionAbortRequest implements Serializable {
    private Timestamp<Long> startTimestamp;

    public TransactionAbortRequest(Timestamp<Long> startTimestamp){
        this.startTimestamp = startTimestamp;
    }

    public Timestamp<Long> getStartTimestamp() {
        return startTimestamp;
    }
}
