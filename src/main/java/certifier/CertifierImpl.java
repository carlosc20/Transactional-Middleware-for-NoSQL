package certifier;

import transaction_manager.BitWriteSet;
import java.util.HashMap;
import java.util.Map;


public class CertifierImpl implements Certifier {

    private Timestamp currentTs;
    //TODO cuidado com clientes que não tentaram dar commit -> forced garbage collection
    //TODO garbage collection

    private Map<Timestamp, IsolatedWrites> history;

    public CertifierImpl() {
        currentTs = new Timestamp(0);
        history = new HashMap<>();
    }

    public CertifierImpl(CertifierImpl certifier){
        this.currentTs = certifier.currentTs;
        this.history = new HashMap<>(certifier.history);
    }

    @Override
    //TODO separar a leitura do currentTs com a criação do isolatedWrites
    public Timestamp start() {
        IsolatedWrites iw = history.get(currentTs);
        if(iw == null){
            iw = new IsolatedWrites();
            history.put(currentTs, iw);
        }
        iw.started();
        return currentTs;
    }

    @Override
    // TODO converter para timestamp
    public Timestamp commit(BitWriteSet ws, Timestamp ts) {
        //TODO pq não i <= currentTs (currentTs até pode ser igual a ts) ?
        for (long i = ts.toLong(); i < currentTs.toLong(); i++) {
            IsolatedWrites iw  = history.get(new Timestamp(i));
            for (BitWriteSet set : iw.getWriteSets()) {
                if(set.intersects(ws)) {
                    history.get(ts).terminated();
                    return new Timestamp(-1);
                }
            }
        }
        IsolatedWrites currentTxs = history.get(currentTs);
        if (currentTxs == null){
            currentTxs =  new IsolatedWrites();
            history.put(currentTs, currentTxs);
            //termina transação
            //TODO e se o flush para a bd correu mal?
            history.get(ts).terminated();
        }
        currentTxs.commit(ws);
        return currentTs;
    }

    @Override
    public void update() {
        currentTs.increment();
    }

    public void setCurrentTs(Timestamp currentTs) {
        this.currentTs = currentTs;
    }

    public void setHistory(Map<Timestamp, IsolatedWrites> history) {
        this.history = new HashMap<>(history);
    }

    public Timestamp getCurrentTs() {
        return currentTs;
    }

    public Map<Timestamp, IsolatedWrites> getHistory() {
        return history;
    }
}
