package transaction_manager;

import certifier.Certifier;
import certifier.Timestamp;
import transaction_manager.raft.FlushAgainInfo;

import java.io.Serializable;
import java.util.Map;

public class State implements Serializable {
    private final Certifier<Long> certifier;
    private final Timestamp<Long> lastLowWaterMark;
    private final Map<Timestamp<Long>, FlushAgainInfo> nonAckedFlushs;

    public State(Certifier<Long> certifier, Timestamp<Long> lastLowWaterMark, Map<Timestamp<Long>, FlushAgainInfo> nonAckedFlushs){
        this.certifier = certifier;
        this.lastLowWaterMark = lastLowWaterMark;
        this.nonAckedFlushs = nonAckedFlushs;
    }

    public Certifier<Long> getCertifier() {
        return certifier;
    }

    public Timestamp<Long> getLastLowWaterMark() {
        return lastLowWaterMark;
    }

    public Map<Timestamp<Long>, FlushAgainInfo> getNonAckedFlushs() {
        return nonAckedFlushs;
    }

    @Override
    public String toString() {
        return "State{" +
                "certifier=" + certifier.toString() + "\n" +
                ", lastNPVSCrash=" + lastLowWaterMark.toPrimitive() + "\n" +
                ", nonAckedFlushs size=" + nonAckedFlushs.size() + "\n" +
                '}';
    }
}
