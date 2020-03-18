package certifier;

public class Timestamp implements Comparable<Timestamp> {

    int ts;

    public Timestamp(int ts) {
        this.ts = ts;
    }

    @Override
    public int compareTo(Timestamp o) {
        return o.ts - ts;
    }
}
