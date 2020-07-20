package transaction_manager;

import certifier.Certifier;
import certifier.Timestamp;

import java.io.Serializable;

public class State implements Serializable {
    private final Certifier<Long> certifier;
    private final Timestamp<Long> lastLowWaterMark;

    public State(Certifier<Long> certifier, Timestamp<Long> lastLowWaterMark){
        this.certifier = certifier;
        this.lastLowWaterMark = lastLowWaterMark;
    }

    public Certifier<Long> getCertifier() {
        return certifier;
    }

    public Timestamp<Long> getLastLowWaterMark() {
        return lastLowWaterMark;
    }

    @Override
    public String toString() {
        return "State{" +
                "certifier=" + certifier.toString() + "\n" +
                ", lastNPVSCrash=" + lastLowWaterMark.toPrimitive() + "\n" +
                '}';
    }
}
