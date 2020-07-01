import nosql.MongoKV;
import org.junit.Test;
import utils.ByteArrayWrapper;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KVTest {


    @Test
    public void readwrite() {
        MongoKV mkv = new MongoKV("mongodb://127.0.0.1:27017", "testeLei", "teste1");

        // writing
        HashMap<ByteArrayWrapper, byte[]> writeMap = new HashMap<>();
        byte[] key = "key".getBytes();
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        byte[] value = "value".getBytes();
        writeMap.put(keyWrapper, value);

        mkv.update(writeMap);


        // reading
        List<byte[]> query = writeMap.keySet().stream().map(ByteArrayWrapper::getData).collect(Collectors.toList());
        query.add("empty".getBytes());

        List<byte[]> result = mkv.scan(query);


        // testing
        Iterator<byte[]> it1 = writeMap.values().iterator();
        Iterator<byte[]> it2 = result.iterator();

        while (it1.hasNext()) {
            assertEquals("Read doesn't match update", new String(it1.next()), new String(it2.next()));
        }
        if(it2.hasNext()) {
            assertNull("Key wasn't written to, should be null", it2.next());
        }

    }

}
