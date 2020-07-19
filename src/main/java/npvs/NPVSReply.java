package npvs;

public class NPVSReply {
    private final boolean success;
    private final byte[] value;

    public NPVSReply(byte[] value) {
        this.success = true;
        this.value = value;
    }

    public NPVSReply(byte[] value, boolean success) {
        this.success = success;
        this.value = value;
    }


    public static NPVSReply FAIL() {
        return new NPVSReply(null, false);
    }

    public boolean isSuccess() {
        return success;
    }

    public byte[] getValue() {
        return value;
    }
}
