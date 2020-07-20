package npvs.binarysearch;

import certifier.Timestamp;
import npvs.AbstractNPVS;
import npvs.NPVS;
import npvs.NPVSReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class NPVSImplBS extends AbstractNPVS {
    private static final Logger LOG = LoggerFactory.getLogger(NPVSImplBS.class);

    private final Map<ByteArrayWrapper, ArrayList<Version>> versionsByKey;

    public NPVSImplBS() {
        super();
        this.versionsByKey = new HashMap<>();
    }

    @Override
    //TODO
    public void evictVersions(Timestamp<Long> lowWaterMark) {

    }

    @Override
    public CompletableFuture<Void> putImpl(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> ts){
        writeMap.forEach((key, v) -> {
            Version newV = new Version(v, ts);
            if (versionsByKey.containsKey(key))
                this.versionsByKey.get(key).add(newV);
            else {
                ArrayList<Version> versions = new ArrayList<>();
                versions.add(newV);
                this.versionsByKey.put(key, versions);
            }
        });
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<NPVSReply> get(ByteArrayWrapper key, Timestamp<Long> ts) {
        if(!versionsByKey.containsKey(key)){
            LOG.info("No such key has been found: {}", key.toString());
            return CompletableFuture.completedFuture(new NPVSReply(null));
        }
        ArrayList<Version> versions = versionsByKey.get(key);
        return CompletableFuture.completedFuture(new NPVSReply(getSICompliantVersion(versions, ts)));
    }

    private byte[] getSICompliantVersion(ArrayList<Version> versions, Timestamp<Long> ts) {
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
