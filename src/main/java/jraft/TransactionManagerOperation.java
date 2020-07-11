package jraft;

import certifier.Timestamp;
import transaction_manager.TransactionManagerService;
import transaction_manager.utils.BitWriteSet;
import transaction_manager.utils.ByteArrayWrapper;

import java.io.Serializable;
import java.util.Map;

public class TransactionManagerOperation implements Serializable {
    /** Get value of timestamp */
    public static final byte  START_TXN = 0x01;
    /** Commits a transaction if no conflict and updates timestamp */
    public static final byte  COMMIT = 0x02;
    public static final byte  DEL_NON_ACK_FLUSH = 0x03;

    private final byte op;
    private final Timestamp<Long> startTimestamp;
    private final Map<ByteArrayWrapper, byte[]> writeMap;
    private final BitWriteSet bws;

    public static TransactionManagerOperation createStartTransaction() {
        return new TransactionManagerOperation(START_TXN);
    }

    public static TransactionManagerOperation createCommit(Timestamp<Long> startTimestamp, Map<ByteArrayWrapper, byte[]> writeMap, BitWriteSet bws) {
        return new TransactionManagerOperation(COMMIT, startTimestamp, writeMap, bws);
    }

    public static TransactionManagerOperation createDeleteNonAckFlush(Timestamp<Long> startTimestamp){
        return new TransactionManagerOperation(DEL_NON_ACK_FLUSH, startTimestamp);
    }

    public TransactionManagerOperation(byte op) {
        this.op = op;
        this.startTimestamp = null;
        this.writeMap = null;
        this.bws = null;
    }

    public TransactionManagerOperation(byte op, Timestamp<Long> startTimestamp, Map<ByteArrayWrapper, byte[]> writeMap, BitWriteSet bws) {
        this.op = op;
        this.startTimestamp = startTimestamp;
        this.writeMap = writeMap;
        this.bws = bws;
    }

    public TransactionManagerOperation(byte op, Timestamp<Long> startTimestamp){
        this.op = op;
        this.startTimestamp = startTimestamp;
        this.writeMap = null;
        this.bws = null;
    }

    public byte getOp() {
        return op;
    }

    public Timestamp<Long> getStartTimestamp() {
        return startTimestamp;
    }

    public BitWriteSet getBws(){return bws;}

    public Map<ByteArrayWrapper, byte[]> getWriteMap() {
        return writeMap;
    }
}