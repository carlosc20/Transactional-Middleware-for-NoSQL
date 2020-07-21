package transaction_manager.raft.sofa_jraft;

import certifier.Timestamp;
import transaction_manager.messaging.TransactionContentMessage;
import transaction_manager.utils.BitWriteSet;
import transaction_manager.utils.ByteArrayWrapper;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;

public class StateMachineOperation implements Serializable {
    /** Get value of timestamp */
    public static final byte  START_TXN = 0x01;
    /** Commits a transaction if no conflict and updates timestamp */
    public static final byte  COMMIT = 0x02;
    public static final byte  UPDATE_STATE = 0x03;
    public static final byte  GARBAGE_COLLECTION = 0x04;
    public static final byte  GET_CURRENT_TIMESTAMP = 0x05;
    public static final byte  ABORT = 0x06;

    private final byte op;
    private final TransactionContentMessage tcm;
    private final Timestamp<Long> timestamp;
    private final LocalDateTime leaderTime;

    public static StateMachineOperation createStartTransaction() {
        return new StateMachineOperation(START_TXN);
    }

    public static StateMachineOperation createGarbageCollection(Timestamp<Long> newLowWaterMark){
        return new StateMachineOperation(GARBAGE_COLLECTION, newLowWaterMark);
    }

    public static StateMachineOperation createCommit(TransactionContentMessage tcm) {
        return new StateMachineOperation(COMMIT, tcm);
    }

    public static StateMachineOperation createAbort(Timestamp<Long> startTimestamp) {
        return new StateMachineOperation(ABORT, startTimestamp);
    }

    public static StateMachineOperation createUpdateState(Timestamp<Long> startTimestamp, Timestamp<Long> commitTimestamp, LocalDateTime leaderTime){
        return new StateMachineOperation(UPDATE_STATE, startTimestamp, commitTimestamp, leaderTime);
    }

    public static StateMachineOperation createGetCurrentTimestamp(){
        return new StateMachineOperation(GET_CURRENT_TIMESTAMP);
    }

    public StateMachineOperation(byte op) {
        this.op = op;
        this.tcm = null;
        this.timestamp = null;
        this.leaderTime = null;
    }

    public StateMachineOperation(byte op, Timestamp<Long> newLowWaterMark) {
        this.op = op;
        this.tcm = null;
        this.timestamp = newLowWaterMark;
        this.leaderTime = null;
    }

    public StateMachineOperation(byte op, TransactionContentMessage tcm) {
        this.op = op;
        this.tcm = tcm;
        this.timestamp = null;
        this.leaderTime = null;
    }

    public StateMachineOperation(byte op, Timestamp<Long> startTimestamp, Timestamp<Long> commitTimestamp, LocalDateTime leaderTime){
        this.op = op;
        this.tcm = new TransactionContentMessage(startTimestamp);
        this.timestamp = commitTimestamp;
        this.leaderTime = leaderTime;
    }

    public byte getOp() {
        return op;
    }

    public Timestamp<Long> getStartTimestamp() {
        assert tcm != null;
        return tcm.getTimestamp();
    }

    public Timestamp<Long> getTimestamp() {
        return timestamp;
    }

    public BitWriteSet getBws(){
        assert tcm != null;
        return tcm.getWriteSet();
    }

    public Map<ByteArrayWrapper, byte[]> getWriteMap() {
        assert tcm != null;
        return tcm.getWriteMap();
    }

    public LocalDateTime getLeaderTime() {
        return leaderTime;
    }

    public TransactionContentMessage getTcm() {
        return tcm;
    }
}