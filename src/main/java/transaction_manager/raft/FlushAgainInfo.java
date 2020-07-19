package transaction_manager.raft;

import certifier.Timestamp;
import npvs.messaging.FlushMessage;

import java.io.Serializable;

public class FlushAgainInfo extends FlushMessage implements Serializable {
    private Timestamp<Long> provisionalCommitTimestamp;

    public FlushAgainInfo(FlushMessage flushMessage, Timestamp<Long> provisionalCommitTimestamp) {
        super(flushMessage);
        this.provisionalCommitTimestamp = provisionalCommitTimestamp;
    }

    public Timestamp<Long> getProvisionalCommitTimestamp() {
        return provisionalCommitTimestamp;
    }
}
