package transaction_manager;

import certifier.Timestamp;
import nosql.KeyValueDriver;
import npvs.NPVS;

import java.util.ArrayList;
import java.util.List;

public class TransactionImpl implements Transaction {


    private NPVS npvs;
    private KeyValueDriver driver;
    private Timestamp ts;
    private WriteSet ws;

    public TransactionImpl(NPVS npvs, KeyValueDriver driver, Timestamp ts) {
        this.ws = new WriteSetImpl();
        this.npvs = npvs;
        this.driver = driver;
        this.ts = ts;
    }

    @Override
    public void flush() {
        ws.optimize();
        //TODO atualizar NPVS com writeset, async???
        //TODO depois atualizar BD com writeset

    }

    @Override
    public void write(byte[] key, byte[] value) {
        if(!ws.contains(key))
            ws.insert(key);
        ws.addWriteOp(key, value);
    }

    @Override
    public void delete(byte[] key) {
        if(!ws.contains(key))
            ws.insert(key);
        ws.addDeleteOp(key);
    }

    @Override
    public byte[] read(byte[] key) {
        // procura no WriteSet da transação, se já tiver alguma operação
        if (ws.contains(key))
            return ws.read(key);

        // procura na BD se a transação mais recente que modificou o item deu commit antes do início desta transação
        byte[] contentBD = driver.read(key);
        if (contentBD != null && ts > contentBD.getCommitTs) // TODO obter value e timestamp do read
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
}
