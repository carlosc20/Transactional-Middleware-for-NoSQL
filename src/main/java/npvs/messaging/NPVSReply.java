package npvs.messaging;

public class NPVSReply {
    private final boolean success;
    private final boolean updatedOutsideSnapshot;
    private final byte[] value;

    public NPVSReply(byte[] value) {
        this.success = true;
        this.updatedOutsideSnapshot = true;
        this.value = value;
    }

    public NPVSReply(byte[] value, boolean success, boolean updatedOutsideSnapshot) {
        this.success = success;
        this.value = value;
        this.updatedOutsideSnapshot = updatedOutsideSnapshot;
    }

    public NPVSReply() {
        this.success = true;
        this.updatedOutsideSnapshot = false;
        this.value = null;
    }

    public static NPVSReply UPTODATE(){return new NPVSReply();}

    public static NPVSReply FAIL() {
        return new NPVSReply(null, false, false);
    }

    public boolean isSuccess() {
        return success;
    }

    public boolean wasUpdatedOutsideSnapshot() {
        return updatedOutsideSnapshot;
    }

    public byte[] getValue() {
        return value;
    }
}
