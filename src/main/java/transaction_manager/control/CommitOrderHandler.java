package transaction_manager.control;

import certifier.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CommitOrderHandler implements CommitControlHandler {
    private static final Logger LOG = LoggerFactory.getLogger(CommitOrderHandler.class);

    //should be a heap
    private List<Timestamp<Long>> batchOrder;
    private ArrayList<FlushAcknowledgment> outOfOrderCommits;
    private boolean outOfOrder;

    public CommitOrderHandler(){
        this.batchOrder = new ArrayList<>();
        this.outOfOrderCommits = new ArrayList<>();
        this.outOfOrder = false;
    }


    @Override
    public CompletableFuture<Void> deliver(Timestamp<Long> commitTs){
        if (commitTs.equals(batchOrder.get(0))){
            LOG.info("Commit is in order TC: " + commitTs.toPrimitive());
            batchOrder.remove(0);
            return CompletableFuture.completedFuture(null);
        }
        else {
            CompletableFuture<Void> new_cf = new CompletableFuture<>();
            outOfOrder = true;
            outOfOrderCommits.add(new FlushAcknowledgment(new_cf, commitTs));
            LOG.info("Commit is out of order TC: " + commitTs.toPrimitive() + " expected: " + batchOrder.get(0).toPrimitive());
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
            if (fa.getCommitTs().equals(batchOrder.get(0))){
                batchOrder.remove(0);
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
    public void putBatch(Timestamp<Long> tc) {
        this.batchOrder.add(tc);
    }

}