package npvs.treemap;

import certifier.Timestamp;
import npvs.AbstractNPVS;
import npvs.NPVS;

import npvs.NPVSReply;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class NPVSImplTM extends AbstractNPVS {
    private final Map<ByteArrayWrapper, TreeMap<Timestamp<Long>, byte[]>> versionsByKey;

    public NPVSImplTM() {
        super();
        this.versionsByKey = new HashMap<>();
    }

    public CompletableFuture<Void> putImpl(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> ts){
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
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<NPVSReply> get(ByteArrayWrapper key, Timestamp<Long> ts) {
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
        return CompletableFuture.completedFuture(new NPVSReply(value));
    }
}
