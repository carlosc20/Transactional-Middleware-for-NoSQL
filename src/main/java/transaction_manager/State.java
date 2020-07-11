package transaction_manager;

import certifier.Certifier;
import certifier.CertifierImpl;
import certifier.MonotonicTimestamp;
import certifier.Timestamp;

public class State {
    private Certifier<Long> certifier;
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

    public void setLastNPVSCrash(Timestamp<Long> lastNPVSCrash) {
        this.lastNPVSCrash = lastNPVSCrash;
    }

    public void setCertifier(Certifier<Long> certifier) {
        this.certifier = certifier;
    }
}
