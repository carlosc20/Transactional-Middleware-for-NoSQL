package transaction_manager;

import certifier.Timestamp;
import nosql.KeyValueDriver;
import npvs.NPVS;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class TransactionImpl implements Transaction {


    private NPVS npvs;
    private KeyValueDriver driver;
    private Timestamp ts;

    private Map<byte[],byte[]> map;

    public TransactionImpl(NPVS npvs, KeyValueDriver driver, Timestamp ts) {
        this.npvs = npvs;
        this.driver = driver;
        this.ts = ts;
    }

    @Override
    public void flush() {
        //TODO atualizar NPVS com writeset, async???
        //TODO depois atualizar BD com writeset

    }

    @Override
    public void write(byte[] key, byte[] value) {
         map.put(key,value);
    }

    @Override
    public void delete(byte[] key) {
        map.put(key,null);
    }

    @Override
    public byte[] read(byte[] key) {
        // procura no WriteSet da transação, se já tiver alguma operação
        if (map.containsKey(key))
            return map.get(key);

        // procura na BD se a transação mais recente que modificou o item deu commit antes do início desta transação
        byte[] contentBD = driver.read(key);
        if (contentBD != null && ts > contentBD.getCommitTs) // TODO ??
            return contentBD;

        // existem concorrentes, se não existe a chave devolve null
        return npvs.read(key, ts);
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

        return ws;
    }
}
