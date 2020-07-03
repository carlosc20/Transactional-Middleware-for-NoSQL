package transaction_manager.messaging;

import nosql.KeyValueDriver;
import npvs.NPVS;

public class ServersContextMessage {
    private final NPVS<Long> npvs;
    private final KeyValueDriver driver;

    public ServersContextMessage(NPVS<Long> npvs, KeyValueDriver driver){
        this.npvs = npvs;
        this.driver = driver;
    }

    public NPVS<Long> getNpvs() {
        return npvs;
    }

    public KeyValueDriver getDriver() {
        return driver;
    }
}
