package certifier;

import transaction_manager.BitWriteSet;

import java.util.ArrayList;
import java.util.List;

public class IsolatedWrites {
    // quantidade de transações que começaram e ainda não acabar no Ts associado a esta classe
    private int numRunning;
    private List<BitWriteSet> writeSets;

    public IsolatedWrites(){
        this.numRunning = 0;
        this.writeSets = new ArrayList<>();
    }

    public void commit(BitWriteSet bws){
        this.writeSets.add(bws);
    }

    public void started(){
        this.numRunning++;
    }

    public void terminated(){
        this.numRunning--;
    }

    //can be garbage collected
    public boolean gcAproved(){
        return this.numRunning == 0;
    }

    public List<BitWriteSet> getWriteSets() {
        return writeSets;
    }
}
