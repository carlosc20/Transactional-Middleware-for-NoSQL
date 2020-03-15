package certifier;

import transaction_manager.Transaction;

public class CertifierStub implements Certifier {

    public CertifierStub(String uri) {

    }

    @Override
    public Timestamp start() {
        return null;
    }

    @Override
    public Timestamp commit(Transaction ws) {
        return null;
    }

    @Override
    public void update() {

    }
}
