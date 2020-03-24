package certifier;

public interface Certifier {
    Timestamp start();
    Timestamp commit(WriteSet ws);
    void update();
}
