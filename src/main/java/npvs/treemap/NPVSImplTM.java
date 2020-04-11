package npvs.treemap;

import npvs.NPVS;
import npvs.binarysearch.Version;
import utils.ByteArrayWrapper;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class NPVSImplTM implements NPVS{
    private Map<ByteArrayWrapper, TreeMap<Long, byte[]>> versionsByKey;

    public NPVSImplTM() {
        this.versionsByKey = new HashMap<>();
    }

    @Override
    public void update(Map<ByteArrayWrapper, byte[]> writeMap, long ts){
        writeMap.forEach((key,v) -> {
            if(versionsByKey.containsKey(key))
                this.versionsByKey.get(key).put(ts, v);
            else{
                TreeMap<Long, byte[]> versions = new TreeMap<>();
                versions.put(ts, v);
                this.versionsByKey.put(key, versions);
            }
        });
        System.out.println(versionsByKey.toString());
    }

    @Override
    public CompletableFuture<byte[]> read(ByteArrayWrapper key, long ts) {
        if(!versionsByKey.containsKey(key)){
            System.out.println("no key");
            return CompletableFuture.completedFuture(null);
        }
        TreeMap<Long, byte[]> versions = versionsByKey.get(key);
        Map.Entry<Long, byte[]> entry = versions.floorEntry(ts);
        byte[] value;
        if(entry == null)
            // last == first por alguma razão.....
            value = versions.lastEntry().getValue();
        else
            value = entry.getValue();
        return CompletableFuture.completedFuture(value);
    }
}
