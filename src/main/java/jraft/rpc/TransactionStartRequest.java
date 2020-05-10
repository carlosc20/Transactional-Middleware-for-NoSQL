package jraft.rpc;

import java.io.Serializable;

public class TransactionStartRequest implements Serializable {
    private static final long serialVersionUID = -6591003954824547594L;

    private boolean readOnlySafe  = true;

    public boolean isReadOnlySafe() {
        return readOnlySafe;
    }

    public void setReadOnlySafe(boolean readOnlySafe) {
        this.readOnlySafe = readOnlySafe;
    }
}
