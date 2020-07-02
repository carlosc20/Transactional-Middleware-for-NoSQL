package jraft;

import java.io.Serializable;

public class CertifierOperation implements Serializable {
    private static final long serialVersionUID = -6597003954824547294L;

    /** Get value of timestamp */
    public static final byte  GET_TS = 0x01;
    /** Commits a transaction if no conflict and updates timestamp */
    public static final byte  COMMIT = 0x02;
/*
    private byte op;
    private MonotonicTimestamp monotonicTimestamp;
    private BitWriteSet bws;

    public static CertifierOperation createGetTimestamp() {
        return new CertifierOperation(GET_TS);
    }

    public static CertifierOperation createCommit(final BitWriteSet bws, final MonotonicTimestamp monotonicTimestamp) {
        return new CertifierOperation(COMMIT, monotonicTimestamp, bws);
    }

    public CertifierOperation(byte op) {
        this.op = op;
        this.monotonicTimestamp = null;
        this.bws = null;
    }

    public CertifierOperation(byte op, MonotonicTimestamp monotonicTimestamp, BitWriteSet bws) {
        this.op = op;
        this.monotonicTimestamp = monotonicTimestamp;
        this.bws = bws;
    }

    public byte getOp() {
        return op;
    }

    public MonotonicTimestamp getMonotonicTimestamp() {
        return monotonicTimestamp;
    }

    public BitWriteSet getBws(){return bws;}

 */
}