package utils;

public class Stats {
    long max = Long.MIN_VALUE;
    long min = Long.MAX_VALUE;
    long average;
    long count = 0;
    long total = 0;

    void addValue(long v) {
        if(v > max) {
            max = v;
        }
        if(v < min) {
            min = v;
        }
        count ++;
        total += v;
        average = total/count;
    }

    @Override
    public String toString() {
        return "{" +
                "max=" + max +
                ", min=" + min +
                ", average=" + average +
                ", count=" + count +
                ", total=" + total +
                '}';
    }
}
