package certifier;

import transaction_manager.BitWriteSet;

import java.util.ArrayList;
import java.util.List;


public class CertifierImpl implements Certifier {

    private long currentTs;
    private List<List<BitWriteSet>> history;

    public CertifierImpl() {
        currentTs = 0;
        history = new ArrayList<>();
    }

    //TODO tudo

    @Override
    public long start() {
        return currentTs;
    }

    @Override
    public long commit(BitWriteSet ws, long ts) {
        for (long i = ts; i < currentTs; i++) {
            List<BitWriteSet> txs = history.get((int)i);
            for (BitWriteSet set : txs) {
                if(set.intersects(ws)) return -1;
            }
        }
        List<BitWriteSet> currentTxs = history.get((int)currentTs);
        if (currentTxs == null){
            currentTxs = new ArrayList<>();
            history.add((int)currentTs, currentTxs);
        }
        currentTxs.add((int)ts,ws);
        return currentTs;
    }

    @Override
    public void update() {
        currentTs++;
    }
}
