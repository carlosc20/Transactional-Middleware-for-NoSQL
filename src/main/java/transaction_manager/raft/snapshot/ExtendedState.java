package transaction_manager.raft.snapshot;

import certifier.Timestamp;
import transaction_manager.State;
import transaction_manager.utils.ByteArrayWrapper;

import java.io.Serializable;
import java.util.Map;

public class ExtendedState implements Serializable {
    private final State standaloneState;
    private final Map<Timestamp<Long>, Map<ByteArrayWrapper, byte[]>> nonAckedFlushs;

    public ExtendedState(State standaloneState, Map<Timestamp<Long>, Map<ByteArrayWrapper, byte[]>> nonAckedFlushs){
        this.standaloneState = standaloneState;
        this.nonAckedFlushs = nonAckedFlushs;
    }

    public State getStandaloneState() {
        return standaloneState;
    }

    public Map<Timestamp<Long>, Map<ByteArrayWrapper, byte[]>> getNonAckedFlushs() {
        return nonAckedFlushs;
    }

    @Override
    public String toString() {
        return "ExtendedState{" +
                "standaloneState=" + standaloneState.toString() + "\n" +
                ", nonAckedFlushs size=" + nonAckedFlushs.size() + "\n" +
                '}';
    }
}
