import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import nosql.MongoAsynchKV;
import nosql.MongoKV;
import nosql.messaging.GetMessage;
import org.junit.Test;
import transaction_manager.utils.ByteArrayWrapper;
import utils.WriteMapsBuilder;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class KVTimestampTest {

    @Test
    public void readwrite() throws ExecutionException, InterruptedException {
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
    }

}
