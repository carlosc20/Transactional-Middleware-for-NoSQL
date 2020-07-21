package runnable_tests;

import certifier.MonotonicTimestamp;
import io.atomix.utils.net.Address;
import npvs.NPVSServer;
import npvs.NPVSStub;
import npvs.messaging.FlushMessage;
import transaction_manager.utils.ByteArrayWrapper;
import utils.WriteMapsBuilder;
import utils.timer.Timer;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

public class NPVSLoadTest {


    public static void main(String[] args) throws InterruptedException, UnknownHostException {
        writeRead(10, 1000);
    }

    public static void writeRead(int putSize, int putCount) throws InterruptedException, UnknownHostException {

        int keyPool = 1000;
        int valuePool = 1000;
        boolean withRaft = false;
        // initializing servers
        List<String> npvsServers = new ArrayList<>();
        npvsServers.add("localhost:20000");
        npvsServers.add("localhost:20001");
        NPVSStub npvs = new NPVSStub(Address.from(10000), npvsServers);
        new NPVSServer(20000,withRaft).start();
        new NPVSServer(20001,withRaft).start();

        Timer timer = new Timer();
        timer.addCheckpoint("start");
        Random rnd = new Random(0);

        CompletableFuture<?>[] futuresPut = new CompletableFuture<?>[putCount];
        for (int i = 0; i < putCount; i++) {

            // Random stuff
            WriteMapsBuilder wmb = new WriteMapsBuilder();
            for (int j = 0; j < putSize; j++) {
                wmb.put(1, String.valueOf(rnd.nextInt(keyPool)), String.valueOf(rnd.nextInt(valuePool)));
            }

            // Get
            //npvs.get(new ByteArrayWrapper(String.valueOf(rnd.nextInt(keyPool)).getBytes()), new MonotonicTimestamp(i + 1));

            // Put
            futuresPut[i] = npvs.put(new FlushMessage(wmb.getWriteMap(1), new MonotonicTimestamp(1 + i), new MonotonicTimestamp(1 + i)));
        }
        CompletableFuture.allOf(futuresPut).join();
        timer.addCheckpoint("puts");



        CompletableFuture<?>[] futuresGet = new CompletableFuture<?>[putCount];
        for (int i = 0; i < putCount; i++) {

            // Random stuff
            WriteMapsBuilder wmb = new WriteMapsBuilder();
            for (int j = 0; j < putSize; j++) {
                wmb.put(1, String.valueOf(rnd.nextInt(keyPool)), String.valueOf(rnd.nextInt(valuePool)));
            }

            // Get
            futuresGet[i] = npvs.get(new ByteArrayWrapper(String.valueOf(rnd.nextInt(keyPool)).getBytes()), new MonotonicTimestamp(i + 1));

            // Put
            //npvs.put(new FlushMessage(wmb.getWriteMap(1), new MonotonicTimestamp(1 + i), new MonotonicTimestamp(1 + i)));
        }
        CompletableFuture.allOf(futuresGet).join();
        timer.addCheckpoint("gets");
        timer.print();
    }
}
