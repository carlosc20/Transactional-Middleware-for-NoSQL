package transaction_manager;

import certifier.Timestamp;
import nosql.KeyValueDriver;
import nosql.messaging.GetMessage;
import npvs.NPVS;
import utils.ByteArrayWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class TransactionImpl implements Transaction {

    private final NPVS<Long> npvs;
    private final KeyValueDriver driver;
    private final Timestamp<Long> ts;
    private boolean latestTimestamp;

    private final Map<ByteArrayWrapper,byte[]> writeMap;

    public TransactionImpl(NPVS<Long> npvs, KeyValueDriver driver, Timestamp<Long> ts) {
        this.writeMap = new HashMap<>();
        this.npvs = npvs;
        this.driver = driver;
        this.ts = ts;
        this.latestTimestamp = true;
    }

    @Override
    public void write(byte[] key, byte[] value) {
         writeMap.put(new ByteArrayWrapper(key), value);
    }

    @Override
    public void delete(byte[] key) {
        writeMap.put(new ByteArrayWrapper(key), null);
    }

    @Override
    public byte[] read(byte[] key) {
        // procura no WriteSet da transação, se já tiver alguma operação
        ByteArrayWrapper k = new ByteArrayWrapper(key);
        if (writeMap.containsKey(k))
            return writeMap.get(k);
        try {
            if (!latestTimestamp)
                return npvs.get(k, ts).get();
            else{
                GetMessage gm = driver.get(k).get();
                if (gm.getTs().isAfter(ts)) {
                    this.latestTimestamp = false;
                    return npvs.get(k, ts).get();
                }
                else return gm.getValue();
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    //TODO fix
    public List<byte[]> scan(List<byte[]> keys) {
        ArrayList<byte[]> list = new ArrayList<>();
        for(byte[] key : keys)
            list.add(read(key));
        return list;
    }
}
