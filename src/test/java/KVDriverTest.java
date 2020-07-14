import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import nosql.MongoAsynchKV;
import nosql.messaging.GetMessage;
import nosql.messaging.ScanMessage;
import org.junit.Test;
import transaction_manager.utils.ByteArrayWrapper;
import utils.WriteMapsBuilder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class KVDriverTest {

    @Test
    public void rwTs() throws ExecutionException, InterruptedException {
        MongoAsynchKV mkv = new MongoAsynchKV("mongodb://127.0.0.1:27017", "testeLei", "teste1");

        WriteMapsBuilder wmb = new WriteMapsBuilder();
        wmb.put(1, "key1", "value1");
        wmb.put(1, "key11", "value11");
        wmb.put(2, "key2", "value2");

        Timestamp<Long> mt1 = new MonotonicTimestamp(100);
        Timestamp<Long> mt2 = new MonotonicTimestamp(200);

        mkv.put(wmb.getWriteMap(1), mt1).get();

        GetMessage gm1 = mkv.get(new ByteArrayWrapper("key1".getBytes())).get();

        assertEquals("Should be equal", 0, gm1.getTs().compareTo(mt1));
        assertEquals("Should be equal", 0, new String(gm1.getValue()).compareTo("value1"));

        mkv.put(wmb.getWriteMap(2), mt2).get();

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
    public void rwKV() throws ExecutionException, InterruptedException {
        MongoAsynchKV mkv = new MongoAsynchKV("mongodb://127.0.0.1:27017", "testeLei", "teste1");

        // writing
        HashMap<ByteArrayWrapper, byte[]> writeMap = new HashMap<>();
        byte[] key = "key".getBytes();
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        byte[] value = "value".getBytes();
        writeMap.put(keyWrapper, value);

        mkv.put(writeMap, new MonotonicTimestamp(1));


        // reading
        Set<ByteArrayWrapper> query = writeMap.keySet();
        query.add(new ByteArrayWrapper("empty".getBytes()));

        CompletableFuture<ScanMessage> result = mkv.scan(writeMap.keySet());


        // testing
        Iterator<byte[]> it1 = writeMap.values().iterator();
        Iterator<byte[]> it2 = result.get().getValues().iterator();

        while (it1.hasNext()) {
            assertEquals("Read doesn't match update", new String(it1.next()), new String(it2.next()));
        }
        if(it2.hasNext()) {
            assertNull("Key wasn't written to, should be null", it2.next());
        }

    }

}
