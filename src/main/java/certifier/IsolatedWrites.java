package certifier;

import transaction_manager.BitWriteSet;

import java.util.ArrayList;
import java.util.List;

public class IsolatedWrites {
    private int numRunning;
    private List<BitWriteSet> writeSets;

    public IsolatedWrites(){
        this.numRunning = 0;
        this.writeSets = new ArrayList<>();
    }

    public void commit(BitWriteSet bws){
        this.numRunning--;
        this.writeSets.add(bws);
    }

    public void started(){
        this.numRunning++;
    }

    public void aborted(){
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
