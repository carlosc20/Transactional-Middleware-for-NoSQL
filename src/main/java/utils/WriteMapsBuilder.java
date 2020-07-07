package utils;

import transaction_manager.utils.BitWriteSet;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.HashMap;

//For testing
public class WriteMapsBuilder {
    private HashMap<Integer, HashMap<ByteArrayWrapper, byte[]>> writeMaps;

    public WriteMapsBuilder(){
        this.writeMaps = new HashMap<>();
    }

    public void put(int id, String key, String value){
        ByteArrayWrapper k = new ByteArrayWrapper(key.getBytes());
        byte[] v = value.getBytes();
        writeMaps.computeIfAbsent(id, x -> new HashMap<>()).put(k,v);
    }

    public HashMap<ByteArrayWrapper, byte[]> getWriteMap(int id){
        return writeMaps.get(id);
    }

    public BitWriteSet getBitWriteSet(int id){
        return new BitWriteSet(writeMaps.get(id).keySet());
    }

    public boolean transfer(int id1, int id2){
        if(writeMaps.containsKey(id1)) {
            HashMap<ByteArrayWrapper, byte[]> map1 = writeMaps.get(id1);
            if(writeMaps.containsKey(id2))
                writeMaps.get(id2).putAll(map1);
            else
                writeMaps.computeIfAbsent(id2, x -> new HashMap<>()).putAll(map1);
            return true;
        }
        return false;
    }
}
