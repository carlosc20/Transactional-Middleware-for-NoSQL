package transaction_manager;


import java.util.Map;
import java.util.Set;

public class WriteSetImpl implements WriteSet {

    private Map<byte[],byte[]> map;

    @Override
    public byte[] read(byte[] key) {
        return map.get(key);
    }

    @Override
    public void write(byte[] key, byte[] value) {
        map.put(key,value);
    }

    @Override
    public void delete(byte[] key) {
        map.put(key,null);
    }

    public Set<byte[]> writeSet() {
        return map.keySet();
    }

}
