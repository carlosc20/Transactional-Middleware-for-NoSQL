package certifier;

import transaction_manager.BitWriteSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CertifierImpl implements Certifier {

    private long currentTs;
    //TODO cuidado com clientes que não tentaram dar commit -> forced garbage collection
    //TODO garbage collection

    private Map<Long, IsolatedWrites> history;

    public CertifierImpl() {
        currentTs = 0;
        history = new HashMap<>();
    }

    @Override
    public long start() {
        IsolatedWrites iw = history.get(currentTs);
        if(iw == null){
            iw = new IsolatedWrites();
            history.put(currentTs, iw);
        }
        iw.started();
        return currentTs;
    }

    @Override
    public long commit(BitWriteSet ws, long ts) {
        //TODO pq não i <= currentTs (currentTs até pode ser igual a ts) ?
        for (long i = ts; i < currentTs; i++) {
            IsolatedWrites iw  = history.get(i);
            for (BitWriteSet set : iw.getWriteSets()) {
                if(set.intersects(ws)) return -1;
            }
        }
        IsolatedWrites currentTxs = history.get(currentTs);
        if (currentTxs == null){
            currentTxs =  new IsolatedWrites();
            history.put(currentTs, currentTxs);
        }
        currentTxs.commit(ws);
        return currentTs;
    }

    @Override
    public void update() {
        currentTs++;
    }
}
