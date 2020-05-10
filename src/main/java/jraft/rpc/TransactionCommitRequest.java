package jraft.rpc;

import transaction_manager.BitWriteSet;

import java.io.Serializable;

public class TransactionCommitRequest implements Serializable {

    private static final long serialVersionUID = -2527003154824547294L;

    private final BitWriteSet bws;
    private final long timestamp;

    public TransactionCommitRequest(BitWriteSet bws, long timestamp){
        this.bws = bws;
        this.timestamp = timestamp;
    }

    public BitWriteSet getBws() {
        return bws;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
