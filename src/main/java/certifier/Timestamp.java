package certifier;

public interface Timestamp<V> extends Comparable<Timestamp<V>> {

    boolean isBefore(Timestamp<V> o);
    boolean isAfter(Timestamp<V> o);
    boolean isBeforeOrEqual(Timestamp<V> o);
    boolean isAfterOrEqual(Timestamp<V> o);
    boolean equals(Timestamp<V> o);
    void increment();
    void add(V quantity);
    V toPrimitive();
    int hashCode();
    void setPrimitive(V new_value);
}
