import certifier.CertifierImpl;
import certifier.Timestamp;
import org.junit.Test;
import transaction_manager.utils.BitWriteSet;
import utils.WriteMapsBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CertifierTest {

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
        CertifierImpl certifier = new CertifierImpl(100);

        List<BitWriteSet> bwss = buildBitWriteSets();

        Timestamp<Long> ts1 = certifier.start();
        Timestamp<Long> ts2 = certifier.start();
        Timestamp<Long> ts3 = certifier.start();

        Timestamp<Long> tc1 = certifier.commit(bwss.get(0), ts1);
        Timestamp<Long> tc2 = certifier.commit(bwss.get(1), ts2);
        Timestamp<Long> tc3 = certifier.commit(bwss.get(2), ts3);

        System.out.println(tc1.toPrimitive());
        System.out.println(tc2.toPrimitive());
        System.out.println(tc3.toPrimitive());

        assertTrue("Shouldn't conflict 1", tc1.toPrimitive() > -1);
        assertTrue("Shouldn't conflict 2", tc2.toPrimitive() > -1);
        assertTrue("Shouldn't conflict 3", tc3.toPrimitive() > -1);
    }

    @Test
    public void certifyWithConflictsV1(){
        CertifierImpl certifier = new CertifierImpl(100);

        List<BitWriteSet> bwss = buildBitWriteSets();

        Timestamp<Long> ts1 = certifier.start();
        Timestamp<Long> ts2 = certifier.start();

        Timestamp<Long> tc1 = certifier.commit(bwss.get(0), ts1);
        Timestamp<Long> tc2 = certifier.commit(bwss.get(1), ts2);
        Timestamp<Long> tc3 = certifier.commit(bwss.get(2), ts1);

        System.out.println(tc1.toPrimitive());
        System.out.println(tc2.toPrimitive());
        System.out.println(tc3.toPrimitive());

        assertTrue("Shouldn't conflict 1", tc1.toPrimitive() > -1);
        assertTrue("Shouldn't conflict 2", tc2.toPrimitive() > -1);
        assertEquals("Should conflict 3", -1, (long) tc3.toPrimitive());
    }
}
