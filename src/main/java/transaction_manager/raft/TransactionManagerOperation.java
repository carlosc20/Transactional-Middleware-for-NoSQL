package transaction_manager.raft;

import certifier.Timestamp;
import transaction_manager.messaging.TransactionContentMessage;
import transaction_manager.utils.BitWriteSet;
import transaction_manager.utils.ByteArrayWrapper;

import java.io.Serializable;
import java.util.Map;

public class TransactionManagerOperation implements Serializable {
    /** Get value of timestamp */
    public static final byte  START_TXN = 0x01;
    /** Commits a transaction if no conflict and updates timestamp */
    public static final byte  COMMIT = 0x02;
    public static final byte  UPDATE_STATE = 0x03;

    private final byte op;
    private final TransactionContentMessage tcm;
    private final Timestamp<Long> currentTimestamp;

    public static TransactionManagerOperation createStartTransaction() {
        return new TransactionManagerOperation(START_TXN);
    }

    public static TransactionManagerOperation createCommit(TransactionContentMessage tcm) {
        return new TransactionManagerOperation(COMMIT, tcm);
    }

    public static TransactionManagerOperation createUpdateState(Timestamp<Long> startTimestamp, Timestamp<Long> commitTimestamp){
        return new TransactionManagerOperation(UPDATE_STATE, startTimestamp, commitTimestamp);
    }

    public TransactionManagerOperation(byte op) {
        this.op = op;
        this.tcm = null;
        this.currentTimestamp = null;
    }

    public TransactionManagerOperation(byte op, TransactionContentMessage tcm) {
        this.op = op;
        this.tcm = tcm;
        this.currentTimestamp = null;
    }

    public TransactionManagerOperation(byte op, Timestamp<Long> startTimestamp, Timestamp<Long> commitTimestamp){
        this.op = op;
        this.tcm = new TransactionContentMessage(startTimestamp);
        this.currentTimestamp = commitTimestamp;
    }

    public byte getOp() {
        return op;
    }

    public Timestamp<Long> getStartTimestamp() {
        assert tcm != null;
        return tcm.getTimestamp();
    }

    public Timestamp<Long> getCurrentTimestamp() {
        return currentTimestamp;
    }

    public BitWriteSet getBws(){
        assert tcm != null;
        return tcm.getWriteSet();
    }

    public Map<ByteArrayWrapper, byte[]> getWriteMap() {
        assert tcm != null;
        return tcm.getWriteMap();
    }

    public TransactionContentMessage getTcm() {
        return tcm;
    }
}