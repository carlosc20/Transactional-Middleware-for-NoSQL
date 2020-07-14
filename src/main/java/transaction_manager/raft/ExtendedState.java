package transaction_manager.raft;

import certifier.Timestamp;
import transaction_manager.State;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.HashMap;
import java.util.Map;

public class ExtendedState extends State {
    private Map<Long, Map<ByteArrayWrapper, byte[]>> nonAckedFlushs;

    public ExtendedState(long timestep){
        super(timestep);
        this.nonAckedFlushs = new HashMap<>();
    }

    public void putFlush(Timestamp<Long> startTimestamp, Map<ByteArrayWrapper, byte[]> writeMap){
        nonAckedFlushs.put(startTimestamp.toPrimitive(), writeMap);
    }

    public void removeFlush(Timestamp<Long> startTimestamp){
        nonAckedFlushs.remove(startTimestamp.toPrimitive());
    }

    public Map<Long, Map<ByteArrayWrapper, byte[]>> getNonAckedFlushs() {
        return nonAckedFlushs;
    }

    public void setNonAckedFlushs(Map<Long, Map<ByteArrayWrapper, byte[]>> nonAckedFlushs) {
        this.nonAckedFlushs = nonAckedFlushs;
    }
}
