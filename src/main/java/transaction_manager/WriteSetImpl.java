package transaction_manager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WriteSetImpl implements WriteSet {

    private Map<byte[],List<Operation>> map;

    @Override
    public boolean contains(byte[] key) {
        return map.containsKey(key);
    }

    @Override
    public void insert(byte[] key) {
        map.put(key, null);
    }

    @Override
    public byte[] read(byte[] key) {
        List<Operation> ops = map.get(key);
        if(ops != null)
            return null;
            // TODO determinar ultimo value
        return null;
    }

    @Override
    public void addWriteOp(byte[] key, byte[] value) {
        List<Operation> ops = map.get(key);
        if(ops != null) {
            ops.add(new WriteOp(value));
        } else {
            ops = new ArrayList<>();
            map.put(key, ops);
        }
    }

    @Override
    public void addDeleteOp(byte[] key) {
        List<Operation> ops = map.get(key);
        if(ops != null) {
            ops.add(new DeleteOp());
        } else {
            ops = new ArrayList<>();
            map.put(key, ops);
        }
    }

    @Override
    public void optimize() {
        //TODO percorrer cada lista e guardar apenas o ultimo valor
    }

    static class Operation {}

    static class WriteOp extends Operation{
        private byte[] value;
        WriteOp(byte[] value){
            this.value = value;
        }
        byte[] getValue() { return value; }
    }

    static class DeleteOp extends Operation{}
}
