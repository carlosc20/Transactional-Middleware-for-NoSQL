package transaction_manager;

import certifier.Timestamp;
import nosql.KeyValueDriver;
import npvs.NPVS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionImpl implements Transaction {

    private NPVS npvs;
    private KeyValueDriver driver;
    private Timestamp ts;

    private Map<byte[],byte[]> writeMap;

    public TransactionImpl(NPVS npvs, KeyValueDriver driver, Timestamp ts) {
        this.writeMap = new HashMap<>();
        this.npvs = npvs;
        this.driver = driver;
        this.ts = ts;
    }

    @Override
    public void flush() {
        npvs.update(writeMap, ts); // async?
        driver.update(writeMap);
    }

    @Override
    public void write(byte[] key, byte[] value) {
         writeMap.put(key,value);
    }

    @Override
    public void delete(byte[] key) {
        writeMap.put(key,null);
    }

    @Override
    public byte[] read(byte[] key) {
        // procura no WriteSet da transação, se já tiver alguma operação
        if (writeMap.containsKey(key))
            return writeMap.get(key);

        // procura no npvs se existem concorrentes, se sim devolve a versao do ts
        // TODO ver se ha commits posteriores ao ts
        npvs.read(key, ts);

        // não existem concorrentes logo vai buscar à BD, se a chave não existe devolve null
        return driver.read(key);
    }

    @Override
    public List<byte[]> scan(List<byte[]> keys) {
        ArrayList<byte[]> list = new ArrayList<>();
        for(byte[] key : keys)
            list.add(read(key));
        return list;
    }

    @Override
    public BitWriteSet getWriteSet() {
        BitWriteSet ws = new BitWriteSet();
        for (byte[] key : writeMap.keySet()) {
            ws.add(key);
        }
        return ws;
    }
}
