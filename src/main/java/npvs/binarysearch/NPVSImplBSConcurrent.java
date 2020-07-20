package npvs.binarysearch;

import certifier.Timestamp;
import npvs.AbstractNPVS;
import npvs.NPVS;
import npvs.NPVSReply;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NPVSImplBSConcurrent extends AbstractNPVS {

    private static final Logger LOG = LoggerFactory.getLogger(NPVSImplBS.class);

    private final Map<ByteArrayWrapper, ArrayList<Version>> versionsByKey;

    private final ExecutorService executor;

    public NPVSImplBSConcurrent() {
        super();
        this.versionsByKey = new ConcurrentHashMap<>();
        this.executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    public NPVSImplBSConcurrent(int threadPoolSize) {
        super();
        this.versionsByKey = new ConcurrentHashMap<>();
        this.executor = Executors.newFixedThreadPool(threadPoolSize);
    }

    @Override
    //TODO
    public void evictVersions(Timestamp<Long> lowWaterMark) {

    }

    @Override
    public CompletableFuture<Void> putImpl(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> ts){

        CompletableFuture<?>[] futures = new CompletableFuture<?>[writeMap.size()];
        int i =  0;
        for (Map.Entry<ByteArrayWrapper, byte[]> entry : writeMap.entrySet()) {
            futures[i] = CompletableFuture.runAsync(()-> {
                Version newV = new Version(entry.getValue(), ts);
                if (versionsByKey.containsKey(entry.getKey()))
                    this.versionsByKey.get(entry.getKey()).add(newV);
                else {
                    ArrayList<Version> versions = new ArrayList<>();
                    versions.add(newV);
                    this.versionsByKey.put(entry.getKey(), versions);
                }

            }, executor);
            i++;
        }
        return CompletableFuture.allOf(futures);
    }

    @Override
    public CompletableFuture<NPVSReply> get(ByteArrayWrapper key, Timestamp<Long> ts) {

        CompletableFuture<NPVSReply> cf = new CompletableFuture<>();

        executor.execute(() -> {
            if(!versionsByKey.containsKey(key)){
                //LOG.info("No such key has been found: {}", key.toString());
                cf.complete(new NPVSReply(null));
                return;
            }
            ArrayList<Version> versions = versionsByKey.get(key);
            cf.complete(new NPVSReply(getSICompliantVersion(versions, ts)));
        });

        return cf;
    }

    private byte[] getSICompliantVersion(@NotNull ArrayList<Version> versions, Timestamp<Long> ts) {
        // só existem versões antigas ou a minha na cabeça do array

        int size = versions.size();
        if (ts.isAfterOrEqual(versions.get(size - 1).ts))
            return versions.get(size - 1).value;
        if (ts.isBefore(versions.get(0).ts))
            return null;
        if (ts.equals(versions.get(0).ts))
            return versions.get(0).value;

        int i = 0, j = size, mid=0;
        while (i < j) {
            mid = (i + j) / 2;
            if (versions.get(mid).ts == ts)
                return versions.get(mid).value;

            if (ts.isAfter(versions.get(mid).ts)) {
                if (mid > 0 && ts.isBefore(versions.get(mid + 1).ts))
                    return versions.get(mid).value;
                j = mid;
            }
            else{
                if (mid < size -1 && ts.isAfterOrEqual(versions.get(mid - 1).ts))
                    return versions.get(mid - 1).value;
                i = mid + 1;
            }
        }
        return versions.get(mid).value;
    }

}
