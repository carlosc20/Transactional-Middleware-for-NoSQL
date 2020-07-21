package transaction_manager;

import certifier.Timestamp;
import nosql.KeyValueDriver;
import nosql.messaging.GetMessage;
import npvs.NPVS;
import npvs.messaging.NPVSReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.messaging.TransactionContentMessage;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class TransactionImpl implements Transaction {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionImpl.class);
    private final NPVS<Long> npvs;
    private final KeyValueDriver driver;
    private final TransactionManager serverStub;
    private final Timestamp<Long> ts;
    private boolean latestTimestamp;

    private final HashMap<ByteArrayWrapper,byte[]> writeMap;

    public TransactionImpl(NPVS<Long> npvs, KeyValueDriver driver, TransactionManager serverStub, Timestamp<Long> ts) {
        this.writeMap = new HashMap<>();
        this.npvs = npvs;
        this.driver = driver;
        this.serverStub = serverStub;
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
    public byte[] read(byte[] key) throws OperationFailedException {
        // procura no WriteSet da transação, se já tiver alguma operação
        ByteArrayWrapper k = new ByteArrayWrapper(key);
        if (writeMap.containsKey(k)) {
            LOG.info("Transaction: {} -> Value of key: {} was found locally",ts.toPrimitive(), k.toString());
            return writeMap.get(k);
        }
        try {
            if (!latestTimestamp){
                return getFromNPVS(k);
            }
            else {
                GetMessage gm = driver.get(k).get();
                if (gm.getTs().isAfter(ts)) {
                    LOG.info("Transaction: {} no longer on latest snapshot view, latest version: {}", ts.toPrimitive(), gm.getTs().toPrimitive());
                    this.latestTimestamp = false;
                    return getFromNPVS(k);
                }
                else {
                    LOG.info("Transaction: {} -> Value of key: {} was fetched from the DB", ts.toPrimitive(), k.toString());
                    return gm.getValue();
                }
            }
        } catch (InterruptedException | ExecutionException | NPVSOutOfDateException e) {
            throw new OperationFailedException();
        }
    }

    private byte[] getFromNPVS(ByteArrayWrapper key) throws NPVSOutOfDateException, ExecutionException, InterruptedException {
        NPVSReply reply = npvs.get(key, ts).get();
        if (!reply.isSuccess()) {
            LOG.info("Transaction: {} -> Value of key: {}, NPVS was out of date", ts.toPrimitive(), key.toString());
            throw new NPVSOutOfDateException();
        }
        LOG.info("Transaction: {} -> Value of key: {} was fetched from NPVS", ts.toPrimitive(), key.toString());
        return reply.getValue();
    }


    @Override
    public List<byte[]> scan(List<byte[]> keys) throws OperationFailedException {
        ArrayList<byte[]> list = new ArrayList<>();
        for(byte[] key : keys)
            list.add(read(key));
        return list;
    }

    @Override
    public Boolean commit() {
        try {
            LOG.info("Transaction: {} committing", ts.toPrimitive());
            Timestamp<Long> ts = serverStub.tryCommit(new TransactionContentMessage(this.writeMap, this.ts)).get();
            return ts.toPrimitive() >= 0; // ts < 0 on error
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Timestamp<Long> commitTs() {
        try {
            LOG.info("Transaction: {} committing", ts.toPrimitive());
            return serverStub.tryCommit(new TransactionContentMessage(this.writeMap, this.ts)).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Timestamp<Long> getTs() {
        return ts;
    }
}
