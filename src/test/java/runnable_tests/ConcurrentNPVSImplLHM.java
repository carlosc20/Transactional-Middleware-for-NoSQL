package runnable_tests;

import certifier.Timestamp;
import npvs.NPVS;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ConcurrentNPVSImplLHM implements NPVS<Long> {
    private final Map<ByteArrayWrapper, LinkedHashMap<Timestamp<Long>, byte[]>> versionsByKey;

    public ConcurrentNPVSImplLHM() {this.versionsByKey = new LinkedHashMap<>();}

    @Override
    public CompletableFuture<Void> put(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> ts) {
        writeMap.forEach((key, v) -> {
            if (versionsByKey.containsKey(key))
                this.versionsByKey.get(key).put(ts, v);
            else {
                LinkedHashMap<Timestamp<Long>, byte[]> versions = new LinkedHashMap<>();
                versions.put(ts, v);
                this.versionsByKey.put(key, versions);
            }
        });
        //System.out.println(versionsByKey.toString());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<byte[]> get(ByteArrayWrapper key, Timestamp<Long> ts) {
        if(!versionsByKey.containsKey(key)){
            //System.out.println("no key");
            return CompletableFuture.completedFuture(null);
        }
        LinkedHashMap<Timestamp<Long>, byte[]> versions = versionsByKey.get(key);
        if(versions.containsKey(ts))
            return CompletableFuture.completedFuture(versions.get(ts));

        byte[] res = null;
        for (Map.Entry<Timestamp<Long>, byte[]> entry : versions.entrySet()) {
            if (entry.getKey().isAfter(ts))
                break;
            res = entry.getValue();
        }
        return CompletableFuture.completedFuture(res);
    }
}
