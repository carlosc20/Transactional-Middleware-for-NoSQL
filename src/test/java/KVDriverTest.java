import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import nosql.KeyValueDriver;
import nosql.MongoAsynchKV;
import nosql.messaging.GetMessage;
import nosql.messaging.ScanMessage;
import org.junit.Test;
import transaction_manager.controll.PipelineWriter;
import transaction_manager.utils.ByteArrayWrapper;
import utils.Timer;
import utils.WriteMapsBuilder;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class KVDriverTest {
    KeyValueDriver mkv = new MongoAsynchKV("mongodb://127.0.0.1:27017", "testeLei", "teste1");

    @Test
    public void readwriteTs() throws ExecutionException, InterruptedException {
        WriteMapsBuilder wmb = new WriteMapsBuilder();
        wmb.put(1, "key1", "value1");
        wmb.put(1, "key11", "value11");
        wmb.put(2, "key2", "value2");

        Timestamp<Long> mt1 = new MonotonicTimestamp(100);
        Timestamp<Long> mt2 = new MonotonicTimestamp(200);

        mkv.put(mt1).get();
        mkv.put(wmb.getWriteMap(1)).get();

        GetMessage gm1 = mkv.get(new ByteArrayWrapper("key1".getBytes())).get();

        assertEquals("Should be equal", 0, gm1.getTs().compareTo(mt1));
        assertEquals("Should be equal", 0, new String(gm1.getValue()).compareTo("value1"));

        mkv.put(mt2).get();
        mkv.put(wmb.getWriteMap(2)).get();

        GetMessage gm2 = mkv.get(new ByteArrayWrapper("key2".getBytes())).get();
        assertEquals("Should be equal", 0, gm2.getTs().compareTo(mt2));
        assertEquals("Should be equal", 0, new String(gm2.getValue()).compareTo("value2"));

        mkv.getWithoutTS(new ByteArrayWrapper("arroz".getBytes())).thenAccept(x ->{
            if (x == null)
                System.out.println("is null");
            else
                System.out.println("not null");
        }).get();
    }


    @Test
    // TODO deletes
    public void readWriteSimple() throws ExecutionException, InterruptedException {
        // writing
        WriteMapsBuilder wmb = new WriteMapsBuilder();
        wmb.put(1, "marco", "dantas");
        wmb.put(1, "bananas", "meloes");
        wmb.put(1, "melancia", "fruta");
        wmb.put(1, "apagado", "fruta");

        HashMap<ByteArrayWrapper, byte[]> writeMap = wmb.getWriteMap(1);
        mkv.put(writeMap).get();

        // reading
        Set<ByteArrayWrapper> query = new HashSet<>(writeMap.keySet());
        query.add(new ByteArrayWrapper("empty".getBytes()));

        ScanMessage result = mkv.scan(query).get();

        // testing
        Iterator<byte[]> it1 = writeMap.values().iterator();
        Iterator<byte[]> it2 = result.getValues().iterator();

        while (it1.hasNext()) {
            byte[] value1 = it1.next();
            byte[] value2 = it2.next();
            assertNotNull("Read was null", value2);
            assertEquals("Read doesn't match update", new String(value1), new String(value2));
        }
        if(it2.hasNext()) {
            assertNull("Key wasn't written to, should be null", it2.next());
        }

    }

    @Test
    public void timestampWritePipe() throws InterruptedException, ExecutionException {
        ExecutorService e = Executors.newFixedThreadPool(8);
        PipelineWriter pipelineWriter = new PipelineWriter(e);
        WriteMapsBuilder wmb = new WriteMapsBuilder();

        for(int j = 0; j < 200; j++)
            wmb.put(1, "marco" + j, "dantas");

        Timer timer = new Timer(TimeUnit.MILLISECONDS);
        timer.start();
        HashMap<ByteArrayWrapper, byte[]> writeMap = wmb.getWriteMap(1);

        int size = 10;
        CompletableFuture<?>[] futures = new CompletableFuture<?>[size];
        for(int i = 0; i < size; i++){
            final long l = i;
            futures[i] = CompletableFuture.supplyAsync(()-> pipelineWriter.put(l, mkv.put(new MonotonicTimestamp(l)), mkv.put(writeMap)),e);
        }
        CompletableFuture.allOf(futures).get();
        timer.print();
    }

    @Test
    public void timestampWriteNoRestrictions() throws ExecutionException, InterruptedException {
        ExecutorService e = Executors.newFixedThreadPool(8);
        WriteMapsBuilder wmb = new WriteMapsBuilder();

        for(int j = 0; j < 70; j++)
            wmb.put(1, "marco" + j, "dantas");

        Timer timer = new Timer(TimeUnit.MILLISECONDS);
        timer.start();
        HashMap<ByteArrayWrapper, byte[]> writeMap = wmb.getWriteMap(1);
        CompletableFuture<?>[] futures = new CompletableFuture<?>[20000];
        for(int i = 0; i < 20000; i+=2){
            final long l = i;
            futures[i] = CompletableFuture.runAsync(() -> mkv.put(new MonotonicTimestamp(l)), e);
            futures[i+1] = CompletableFuture.runAsync(() -> mkv.put(writeMap), e);
        }
        CompletableFuture.allOf(futures).get();
        timer.print();
    }

    @Test
    public void timestampWriteSequential() throws ExecutionException, InterruptedException {
        WriteMapsBuilder wmb = new WriteMapsBuilder();

        for(int j = 0; j < 70; j++)
            wmb.put(1, "marco" + j, "dantas");

        Timer timer = new Timer(TimeUnit.MILLISECONDS);
        timer.start();
        HashMap<ByteArrayWrapper, byte[]> writeMap = wmb.getWriteMap(1);
        for (int i = 0; i < 10000; i += 1) {
            final long l = i;
            mkv.put(new MonotonicTimestamp(l))
                    .thenAccept(x -> mkv.put(writeMap)).get();
        }
        timer.print();
    }

}
