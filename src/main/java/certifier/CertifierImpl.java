package certifier;

import transaction_manager.BitWriteSet;

import java.util.ArrayList;
import java.util.List;

public class CertifierImpl implements Certifier {

    private List<BitWriteSet> history;

    @Override
    public Timestamp start() {
        // TODO emite novo timestamp
        return null;
    }

    @Override
    public Timestamp commit(BitWriteSet ws) {
        // TODO vê se é incompativel com transações concorrentes
        return null;
    }

    @Override
    public void update() {
        // TODO atuliza timestamp, timestamp como argumento?
    }
}
