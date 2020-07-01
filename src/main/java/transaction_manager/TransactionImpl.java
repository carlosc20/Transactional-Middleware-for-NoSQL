package transaction_manager;

import certifier.Timestamp;
import nosql.KeyValueDriver;
import npvs.NPVS;
import utils.ByteArrayWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionImpl implements Transaction {

    private NPVS npvs;
    private KeyValueDriver driver;
    private Timestamp ts;

    private Map<ByteArrayWrapper,byte[]> writeMap;

    public TransactionImpl(NPVS npvs, KeyValueDriver driver, Timestamp ts) {
        this.writeMap = new HashMap<>();
        this.npvs = npvs;
        this.driver = driver;
        this.ts = ts;
    }

    @Override
    public void flush(Timestamp tc) {
        npvs.update(writeMap, tc); // async?
        driver.update(writeMap);
    }

    @Override
    public Timestamp getStartTimestamp() {
        return ts;
    }

    @Override
    public void write(byte[] key, byte[] value) {
         writeMap.put(new ByteArrayWrapper(key),value);
    }

    @Override
    public void delete(byte[] key) {
        writeMap.put(new ByteArrayWrapper(key),null);
    }

    @Override
    public byte[] read(byte[] key) {
        // procura no WriteSet da transação, se já tiver alguma operação
        if (writeMap.containsKey(new ByteArrayWrapper(key)))
            return writeMap.get(new ByteArrayWrapper(key));

        // procura no npvs se existem concorrentes, se sim devolve a versao do ts
        // TODO ver se ha commits posteriores ao ts
        npvs.read(new ByteArrayWrapper(key), ts);

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
        for (ByteArrayWrapper key : writeMap.keySet()) {
            ws.add(key.getData());
        }
        return ws;
    }
}
