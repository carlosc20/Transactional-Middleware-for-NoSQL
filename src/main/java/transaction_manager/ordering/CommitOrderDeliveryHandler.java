package transaction_manager.ordering;

import certifier.MonotonicTimestamp;
import certifier.Timestamp;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public class CommitOrderDeliveryHandler {
    private Timestamp<Long> lastArrival;
    private ArrayList<FlushAcknowledgment> outOfOrderCommits;
    private long timestep;
    private boolean outOfOrder;

    public CommitOrderDeliveryHandler(){
        this.lastArrival = new MonotonicTimestamp(0);
        this.outOfOrderCommits = new ArrayList<>();
        this.timestep = 1;
        this.outOfOrder = false;
    }

    public CommitOrderDeliveryHandler(long timestep){
        this();
        this.timestep = timestep;
    }

    public CompletableFuture<Void> returnInOrder(Timestamp<Long> commitTs){
        if (commitTs.isRightAfter(lastArrival, timestep)){
            lastArrival.add(timestep);
            return CompletableFuture.completedFuture(null);
        }
        else {
            CompletableFuture<Void> new_cf = new CompletableFuture<>();
            outOfOrder = true;
            outOfOrderCommits.add(new FlushAcknowledgment(new_cf, commitTs));
            return new_cf;
        }
    }

    //TODO OTIMIZAR
    public void completeNewInOrder(){
        if(!outOfOrder)
            return;
        outOfOrderCommits.sort(Comparator.comparing(FlushAcknowledgment::getCommitTs));
        Iterator<FlushAcknowledgment> iter = outOfOrderCommits.iterator();
        while (iter.hasNext()){
            FlushAcknowledgment fa = iter.next();
            if (fa.getCommitTs().isRightAfter(this.lastArrival, timestep)){
                this.lastArrival.add(timestep);
                fa.getCf().complete(null);
                iter.remove();
            }
            else
                break;
        }
        if (outOfOrderCommits.size() == 0)
            outOfOrder = false;
    }
}