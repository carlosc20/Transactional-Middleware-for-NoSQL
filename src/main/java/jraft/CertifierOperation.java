package jraft;

import transaction_manager.BitWriteSet;

import java.io.Serializable;

public class CertifierOperation implements Serializable {

    private static final long serialVersionUID = -6597003954824547294L;

    /** Get value of timestamp */
    public static final byte  GET_TS = 0x01;
    /** Commits a transaction if no conflict and updates timestamp */
    public static final byte  COMMIT = 0x02;

    private byte op;
    private long timestamp;
    private BitWriteSet bws;

    public static CertifierOperation createGetTimestamp() {
        return new CertifierOperation(GET_TS);
    }

    public static CertifierOperation createCommit(BitWriteSet bws, final long timestamp) {
        return new CertifierOperation(COMMIT, timestamp, bws);
    }

    public CertifierOperation(byte op) {
        this.op = op;
        this.timestamp = -1;
        this.bws = null;
    }

    public CertifierOperation(byte op, long timestamp, BitWriteSet bws) {
        this.op = op;
        this.timestamp = timestamp;
        this.bws = bws;
    }

    public byte getOp() {
        return op;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public BitWriteSet getBws(){return bws;}
}