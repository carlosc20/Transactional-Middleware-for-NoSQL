package transaction_manager.control;

import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public class CommitOrderHandler implements CommitControlHandler {
    private static final Logger LOG = LoggerFactory.getLogger(CommitOrderHandler.class);

    private Timestamp<Long> lastArrival;
    private ArrayList<FlushAcknowledgment> outOfOrderCommits;
    private long timestep;
    private boolean outOfOrder;

    public CommitOrderHandler(){
        this.lastArrival = new MonotonicTimestamp(0);
        this.outOfOrderCommits = new ArrayList<>();
        this.timestep = 1;
        this.outOfOrder = false;
    }

    public CommitOrderHandler(long timestep){
        this();
        this.timestep = timestep;
    }

    @Override
    public CompletableFuture<Void> deliver(Timestamp<Long> commitTs){
        if (commitTs.isRightAfter(lastArrival, timestep)){
            LOG.info("Commit is in order TC: " + commitTs.toPrimitive());
            lastArrival.add(timestep);
            return CompletableFuture.completedFuture(null);
        }
        else {
            CompletableFuture<Void> new_cf = new CompletableFuture<>();
            outOfOrder = true;
            outOfOrderCommits.add(new FlushAcknowledgment(new_cf, commitTs));
            long expected = lastArrival.toPrimitive() + timestep;
            LOG.info("Commit is out of order TC: " + commitTs.toPrimitive() + " expected: " + expected);
            return new_cf;
        }
    }

    @Override
    public void completeDeliveries(){
        if(!outOfOrder)
            return;
        Iterator<FlushAcknowledgment> iter = outOfOrderCommits.iterator();
        while (iter.hasNext()){
            FlushAcknowledgment fa = iter.next();
            if (fa.getCommitTs().isRightAfter(this.lastArrival, timestep)){
                this.lastArrival.add(timestep);
                fa.getCf().complete(null);
                iter.remove();
                completeDeliveries();
                return;
            }
        }
        if (outOfOrderCommits.size() == 0)
            outOfOrder = false;
    }

    @Override
    public void setTimestamp(Timestamp<Long> tc) {
        this.lastArrival.set(tc);
    }

}