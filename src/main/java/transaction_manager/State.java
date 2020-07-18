package transaction_manager;

import certifier.Certifier;
import certifier.Timestamp;

import java.io.Serializable;

public class State implements Serializable {
    private final Certifier<Long> certifier;
    private final Timestamp<Long> lastNPVSCrash;

    public State(Certifier<Long> certifier, Timestamp<Long> lastNPVSCrash){
        this.certifier = certifier;
        this.lastNPVSCrash = lastNPVSCrash;
    }

    public Certifier<Long> getCertifier() {
        return certifier;
    }

    public Timestamp<Long> getLastNPVSCrash() {
        return lastNPVSCrash;
    }
}
