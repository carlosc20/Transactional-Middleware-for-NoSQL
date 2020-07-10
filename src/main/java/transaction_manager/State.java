package transaction_manager;

import certifier.Certifier;
import certifier.CertifierImpl;
import certifier.MonotonicTimestamp;
import certifier.Timestamp;

public class State {
    private final Certifier<Long> certifier;
    private Timestamp<Long> lastNPVSCrash;

    public State(long timestep){
        certifier = new CertifierImpl(timestep);
        lastNPVSCrash = new MonotonicTimestamp(-1);
    }

    public Certifier<Long> getCertifier() {
        return certifier;
    }

    public Timestamp<Long> getLastNPVSCrash() {
        return lastNPVSCrash;
    }
}
