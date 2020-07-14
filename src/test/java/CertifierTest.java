import certifier.IntervalCertifierImpl;
import certifier.Timestamp;
import org.junit.Test;
import transaction_manager.utils.BitWriteSet;
import utils.WriteMapsBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class CertifierTest {

    private IntervalCertifierImpl certifier;

    public CertifierTest()
    {
        this.certifier = new IntervalCertifierImpl(100);
    }

    private boolean certifierCommit(BitWriteSet bws, Timestamp<Long> ts){
        Timestamp<Long> tc = this.certifier.commit(bws, ts);
        if (tc.toPrimitive() > -1) {
            System.out.println("Transaction with ts: " + ts.toPrimitive() + " has tc: " + tc.toPrimitive());
            this.certifier.update();
            return true;
        }
        return false;
    }

    private List<BitWriteSet> buildBitWriteSets(){
        WriteMapsBuilder wmb = new WriteMapsBuilder();
        ArrayList<BitWriteSet> res = new ArrayList<>();

        wmb.put(1, "marco", "dantas");
        wmb.put(1, "daniel", "vilar");
        wmb.put(1, "carlos", "castro");

        wmb.put(2, "marco2", "dantas2");
        wmb.put(2, "cesar", "borges");

        wmb.put(3, "josé", "fernandes");
        wmb.put(3, "marco", "dantas");

        res.add(wmb.getBitWriteSet(1));
        res.add(wmb.getBitWriteSet(2));
        res.add(wmb.getBitWriteSet(3));

        return res;
    }

    @Test
    public void certifyWithNoConflicts(){
        List<BitWriteSet> bwss = buildBitWriteSets();

        Timestamp<Long> ts1 = certifier.start();
        assertTrue("Shouldn't conflict 1", certifierCommit(bwss.get(0), ts1));
        Timestamp<Long> ts2 = certifier.start();
        assertTrue("Shouldn't conflict 2", certifierCommit(bwss.get(1), ts2));
        Timestamp<Long> ts3 = certifier.start();
        assertTrue("Shouldn't conflict 3", certifierCommit(bwss.get(2), ts3));
    }

    @Test
    public void certifyWithConflictsV1(){
        List<BitWriteSet> bwss = buildBitWriteSets();

        Timestamp<Long> ts1 = certifier.start();
        Timestamp<Long> ts2 = certifier.start();

        System.out.println("Commit 1");
        assertTrue("Shouldn't conflict 1", certifierCommit(bwss.get(0), ts1));
        System.out.println("Commit 2");
        assertTrue("Shouldn't conflict 2", certifierCommit(bwss.get(1), ts2));
        System.out.println("Commit 3");
        assertFalse("Should conflict 3", certifierCommit(bwss.get(2), ts1));
        Timestamp<Long> ts3 = certifier.start();
        assertTrue("Shouldn't conflict 4", certifierCommit(bwss.get(2), ts3));
    }
}
