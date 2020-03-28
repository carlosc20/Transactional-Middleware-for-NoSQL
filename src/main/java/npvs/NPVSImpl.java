package npvs;

import certifier.Timestamp;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class NPVSImpl implements NPVS {
    //TODO comparar o custo com várias versões desta estrutura
    private Map<byte[], Map<Timestamp, byte[]>> versions;


    public NPVSImpl() {
        this.versions = new HashMap<>();
    }

    @Override
    public void update(Map<byte[], byte[]> writeMap, Timestamp ts){
        writeMap.forEach((k,v) -> {
            if(versions.get(k).containsKey(ts))
                this.versions.get(k).put(ts, v);
            else{
                Map<Timestamp, byte[]> m = new HashMap<>();
                m.put(ts, v);
                this.versions.put(k, m);
            }
        });
    }

    @Override
    public CompletableFuture<byte[]> read(byte[] key, Timestamp ts) {
        //versão pode não estar por algum erro?
        if(!versions.containsKey(key))
            return null;
        return CompletableFuture.completedFuture(this.versions.get(key).get(ts));
    }
}
