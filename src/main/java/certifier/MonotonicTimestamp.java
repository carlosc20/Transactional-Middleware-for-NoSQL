package certifier;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class MonotonicTimestamp implements Timestamp<Long>{

    private long ts;

    public MonotonicTimestamp(long ts) {
        this.ts = ts;
    }

    public MonotonicTimestamp(Timestamp<Long> mt){this.ts = mt.toPrimitive();}

    @Override
    public int compareTo(@NotNull Timestamp<Long> o) {
        return Long.compare(ts, o.toPrimitive());
    }

    @Override
    public boolean isBefore(Timestamp<Long> o) {
        return o.toPrimitive() > ts;
    }

    @Override
    public boolean isAfter(Timestamp<Long> o) {
        return o.toPrimitive() < ts;
    }

    @Override
    public boolean isBeforeOrEqual(Timestamp<Long> o) {
        return o.toPrimitive() >= ts;
    }

    @Override
    public boolean isAfterOrEqual(Timestamp<Long> o) {
        return o.toPrimitive() <= ts;
    }

    @Override
    public void increment(){
        ts++;
    }

    @Override
    public void add(Long quantity) {
        ts += quantity;
    }

    @Override
    public Long toPrimitive() {
        return ts;
    }

    @Override
    public boolean equals(Timestamp<Long> o) {
        return ts == o.toPrimitive();
    }

    @Override
    public int hashCode() {
        return Objects.hash(ts);
    }

    @Override
    public void setPrimitive(Long new_value) {
        ts = new_value;
    }
}
