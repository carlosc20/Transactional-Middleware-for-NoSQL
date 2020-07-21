package transaction_manager.messaging;

import java.io.Serializable;

public class TransactionStartRequest implements Serializable {
    private boolean readOnlySafe  = true;

    public boolean isReadOnlySafe() {
        return readOnlySafe;
    }

    public void setReadOnlySafe(boolean readOnlySafe) {
        this.readOnlySafe = readOnlySafe;
    }
}
