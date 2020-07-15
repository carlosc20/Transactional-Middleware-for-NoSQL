package transaction_manager.raft.snapshot;

import transaction_manager.State;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.Map;

public class ExtendedState {
    private final State standaloneState;
    private final Map<Long, Map<ByteArrayWrapper, byte[]>> nonAckedFlushs;

    public ExtendedState(State standaloneState, Map<Long, Map<ByteArrayWrapper, byte[]>> nonAckedFlushs){
        this.standaloneState = standaloneState;
        this.nonAckedFlushs = nonAckedFlushs;
    }

    public State getStandaloneState() {
        return standaloneState;
    }

    public Map<Long, Map<ByteArrayWrapper, byte[]>> getNonAckedFlushs() {
        return nonAckedFlushs;
    }
}
