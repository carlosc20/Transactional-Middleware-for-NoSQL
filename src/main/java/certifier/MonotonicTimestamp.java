package certifier;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Objects;

public class MonotonicTimestamp implements Timestamp<Long>{

    private Long ts;

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
    public boolean isRightAfter(Timestamp<Long> o, Long interval) {
        return ts - interval == o.toPrimitive();
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
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        MonotonicTimestamp that = (MonotonicTimestamp) o;

        return new EqualsBuilder()
                .append(ts, that.ts)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return ts.hashCode();
    }

    @Override
    public void setPrimitive(Long new_value) {
        ts = new_value;
    }

    @Override
    public void set(Timestamp<Long> o) {
        ts = o.toPrimitive();
    }
}
