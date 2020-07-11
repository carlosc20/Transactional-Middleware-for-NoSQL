package jraft;

import certifier.Timestamp;
import transaction_manager.State;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.HashMap;

public class ExtendedState extends State {
    private HashMap<Long, HashMap<ByteArrayWrapper, byte[]>> nonAckedFlushs;

    public ExtendedState(long timestep){
        super(timestep);
        this.nonAckedFlushs = new HashMap<>();
    }

    public void putFlush(Timestamp<Long> startTimestamp, HashMap<ByteArrayWrapper, byte[]> writeMap){
        nonAckedFlushs.put(startTimestamp.toPrimitive(), writeMap);
    }

    public void removeFlush(Timestamp<Long> startTimestamp){
        nonAckedFlushs.remove(startTimestamp.toPrimitive());
    }

    public HashMap<Long, HashMap<ByteArrayWrapper, byte[]>> getNonAckedFlushs() {
        return nonAckedFlushs;
    }

    public void setNonAckedFlushs(HashMap<Long, HashMap<ByteArrayWrapper, byte[]>> nonAckedFlushs) {
        this.nonAckedFlushs = nonAckedFlushs;
    }
}
