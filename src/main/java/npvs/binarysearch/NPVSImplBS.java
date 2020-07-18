package npvs.binarysearch;

import certifier.Timestamp;
import npvs.NPVS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class NPVSImplBS implements NPVS<Long> {
    //TODO comparar o custo com várias versões desta estrutura
    private static final Logger LOG = LoggerFactory.getLogger(NPVSImplBS.class);
    //ArrayList + binarySearch:
    // O(1) para adicionar
    // O(x) para remover -> vai depender do que for utilizado
    // O(log n) para procura (em média)

    // Nota -> treeMap talvez tenha melhores resultados, por causa da inserção
    // Caso numero de procuras seja reduzido o melhor seria alguma espécie de lista ligada

    private final Map<ByteArrayWrapper, ArrayList<Version>> versionsByKey;


    public NPVSImplBS() {
        this.versionsByKey = new HashMap<>();
    }

    @Override
    public CompletableFuture<Void> put(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> ts){
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
    public CompletableFuture<byte[]> get(ByteArrayWrapper key, Timestamp<Long> ts) {
        if(!versionsByKey.containsKey(key)){
            LOG.info("No such key has been found: {}", key.toString());
            return CompletableFuture.completedFuture(null);
        }
        ArrayList<Version> versions = versionsByKey.get(key);
        return CompletableFuture.completedFuture(getSICompliantVersion(versions, ts));
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
