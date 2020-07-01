package npvs.linkedhashmap;

import certifier.Timestamp;
import npvs.NPVS;
import utils.ByteArrayWrapper;

import java.sql.Time;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import java.util.concurrent.CompletableFuture;

public class NPVSImplLHM implements NPVS {
    private Map<ByteArrayWrapper, LinkedHashMap<Timestamp, byte[]>> versionsByKey;


    public NPVSImplLHM() {this.versionsByKey = new LinkedHashMap<>();}


    @Override
    public void update(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp ts) {
        writeMap.forEach((key,v) -> {
            if(versionsByKey.containsKey(key))
                this.versionsByKey.get(key).put(ts, v);
            else{
                LinkedHashMap<Timestamp, byte[]> versions = new LinkedHashMap<>();
                versions.put(ts, v);
                this.versionsByKey.put(key, versions);
            }
        });
        System.out.println(versionsByKey.toString());
    }

    @Override
    public CompletableFuture<byte[]> read(ByteArrayWrapper key, Timestamp ts) {
        if(!versionsByKey.containsKey(key)){
            System.out.println("no key");
            return CompletableFuture.completedFuture(null);
        }
        LinkedHashMap<Timestamp, byte[]> versions = versionsByKey.get(key);
        if(versions.containsKey(ts))
            return CompletableFuture.completedFuture(versions.get(ts));

        byte[] res = null;
        Iterator<Map.Entry<Timestamp, byte[]>> entries = versions.entrySet().iterator();
        while(entries.hasNext()){
            Map.Entry<Timestamp, byte[]> entry = entries.next();
            if(entry.getKey().isAfter(ts))
                break;
            res = entry.getValue();
        }
        return CompletableFuture.completedFuture(res);
    }
}
