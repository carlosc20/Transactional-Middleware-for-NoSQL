package jraft.rpc;

public class UpdateTimestampRequest {
    private boolean readOnlySafe  = false;

    public boolean isReadOnlySafe() {
        return readOnlySafe;
    }

    public void setReadOnlySafe(boolean readOnlySafe) {
        this.readOnlySafe = readOnlySafe;
    }
}
