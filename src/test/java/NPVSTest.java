import certifier.MonotonicTimestamp;
import io.atomix.utils.net.Address;
import npvs.NPVSReply;
import npvs.NPVSServer;
import npvs.NPVSStub;
import org.junit.Test;
import spread.SpreadException;
import transaction_manager.utils.ByteArrayWrapper;
import utils.WriteMapsBuilder;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class NPVSTest {

    private final List<String> npvsServers = new ArrayList<>();
    private final NPVSStub npvs = new NPVSStub(Address.from(10000), npvsServers);


    public void fetchAndAssert(long timestamp, ByteArrayWrapper baw, String str){
        npvs.get(baw, new MonotonicTimestamp(timestamp))
            .thenAccept(x -> {
                String new_str = new String(x.getValue());
                System.out.println(new_str + " Arrived! on request with ts: " + timestamp);
                assertEquals("Should be equal", 0, new_str.compareTo(str));
            });
    }

    @Test
    public void writeRead() throws InterruptedException, SpreadException, UnknownHostException {

        npvsServers.add("localhost:20000");
        npvsServers.add("localhost:20001");
        new NPVSServer(20000, 40000, "0").start();
        new NPVSServer(20001, 40000, "1").start();

        WriteMapsBuilder wmb = new WriteMapsBuilder();
        ExecutorService taskExecutor = Executors.newFixedThreadPool(8);

        ByteArrayWrapper baw = new ByteArrayWrapper("marco".getBytes());
        wmb.put(1, "marco", "dantas");
        wmb.put(1, "zé", "machado");
        wmb.put(2, "marco", "dantas2");
        wmb.put(4, "marco", "dantas4");
        wmb.put(10, "marco", "dantas10");

        npvs.put(wmb.getWriteMap(1), new MonotonicTimestamp(1));
        npvs.put(wmb.getWriteMap(2), new MonotonicTimestamp(2));
        npvs.put(wmb.getWriteMap(4), new MonotonicTimestamp(4));
        npvs.put(wmb.getWriteMap(10), new MonotonicTimestamp(10));

        Thread.sleep(2000); //para todos os writes serem efetivos

        fetchAndAssert(3, baw, "dantas2");
        fetchAndAssert(5, baw, "dantas4");
        fetchAndAssert(1, baw, "dantas");
        fetchAndAssert(9, baw, "dantas4");
        fetchAndAssert(11, baw, "dantas10");
        fetchAndAssert(10, baw, "dantas10");

        taskExecutor.awaitTermination(10, TimeUnit.SECONDS);
    }
}
