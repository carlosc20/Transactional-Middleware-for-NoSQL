package transaction_manager;

public interface Transaction {

    void flush();
    void write(key, value);
    void delete(key);
    Object read(key, Ts);
    List<Object> read(List keys, Ts);
}
