package npvs.treemap;

import certifier.Timestamp;
import npvs.NPVS;

import utils.ByteArrayWrapper;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class NPVSImplTM implements NPVS<Long>{
    private final Map<ByteArrayWrapper, TreeMap<Timestamp<Long>, byte[]>> versionsByKey;

    public NPVSImplTM() {
        this.versionsByKey = new HashMap<>();
    }

    @Override
    public CompletableFuture<Boolean> put(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> ts){
        try {
            writeMap.forEach((key, v) -> {
                if (versionsByKey.containsKey(key))
                    this.versionsByKey.get(key).put(ts, v);
                else {
                    TreeMap<Timestamp<Long>, byte[]> versions = new TreeMap<>();
                    versions.put(ts, v);
                    this.versionsByKey.put(key, versions);
                }
            });
            System.out.println(versionsByKey.toString());
            return CompletableFuture.completedFuture(true);
        }catch (Exception e){
            return CompletableFuture.completedFuture(false);
        }
    }

    @Override
    public CompletableFuture<byte[]> get(ByteArrayWrapper key, Timestamp<Long> ts) {
        if(!versionsByKey.containsKey(key)){
            System.out.println("no key");
            return CompletableFuture.completedFuture(null);
        }
        TreeMap<Timestamp<Long>, byte[]> versions = versionsByKey.get(key);
        Map.Entry<Timestamp<Long>, byte[]> entry = versions.floorEntry(ts);
        byte[] value;
        if(entry == null)
            // last == first por alguma razão.....
            value = versions.lastEntry().getValue();
        else
            value = entry.getValue();
        return CompletableFuture.completedFuture(value);
    }
}
