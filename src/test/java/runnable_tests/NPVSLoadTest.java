package runnable_tests;

import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import io.atomix.utils.net.Address;
import npvs.NPVSServer;
import npvs.NPVSStub;
import npvs.messaging.FlushMessage;
import spread.SpreadException;
import transaction_manager.utils.ByteArrayWrapper;
import utils.WriteMapsBuilder;
import utils.timer.Timer;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NPVSLoadTest {


    public static void main(String[] args) throws InterruptedException, SpreadException, UnknownHostException {
        writeRead(10, 50000);
    }

    public static void writeRead(int putSize, int putCount) throws InterruptedException, SpreadException, UnknownHostException {

        int keyPool = 1000;
        int valuePool = 1000;

        // initializing servers
        List<String> npvsServers = new ArrayList<>();
        npvsServers.add("localhost:20000");
        npvsServers.add("localhost:20001");
        NPVSStub npvs = new NPVSStub(Address.from(10000), npvsServers);
        new NPVSServer(20000).start();
        new NPVSServer(20001).start();

        Timer timer = new Timer();
        timer.addCheckpoint("start");
        Random rnd = new Random(0);

        for (int i = 0; i < putCount; i++) {
            // Random stuff
            WriteMapsBuilder wmb = new WriteMapsBuilder();
            for (int j = 0; j < putSize; j++) {
                wmb.put(1, String.valueOf(rnd.nextInt(keyPool)), String.valueOf(rnd.nextInt(valuePool)));
            }

            // Get
            //npvs.get(new ByteArrayWrapper(String.valueOf(rnd.nextInt(keyPool)).getBytes()), new MonotonicTimestamp(i + 1));

            // Put
            npvs.put(new FlushMessage(wmb.getWriteMap(1), new MonotonicTimestamp(1 + i), new MonotonicTimestamp(1 + i)));
        }
        timer.addCheckpoint("puts");

        for (int i = 0; i < putCount; i++) {
            // Random stuff
            WriteMapsBuilder wmb = new WriteMapsBuilder();
            for (int j = 0; j < putSize; j++) {
                wmb.put(1, String.valueOf(rnd.nextInt(keyPool)), String.valueOf(rnd.nextInt(valuePool)));
            }

            // Get
            npvs.get(new ByteArrayWrapper(String.valueOf(rnd.nextInt(keyPool)).getBytes()), new MonotonicTimestamp(i + 1));

            // Put
            //npvs.put(new FlushMessage(wmb.getWriteMap(1), new MonotonicTimestamp(1 + i), new MonotonicTimestamp(1 + i)));
        }
        timer.addCheckpoint("gets");
        timer.print();
    }
}
