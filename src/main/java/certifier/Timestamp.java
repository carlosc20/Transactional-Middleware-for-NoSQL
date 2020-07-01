package certifier;

public class Timestamp implements Comparable<Timestamp> {

    private long ts;

    public Timestamp(long ts) {
        this.ts = ts;
    }

    @Override
    public int compareTo(Timestamp o) {
        return (int) (o.ts - ts);
    }

    public boolean isBefore(Timestamp o) {
        return o.toLong() > ts;
    }

    public boolean isAfter(Timestamp o) {
        return o.toLong() < ts;
    }

    public void increment(){
        ts++;
    }

    public long toLong() {
        return ts;
    }
}
