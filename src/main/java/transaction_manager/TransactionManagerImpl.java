package transaction_manager;

import certifier.Certifier;
import certifier.CertifierStub;
import certifier.Timestamp;
import nosql.KeyValueDriver;
import nosql.MongoKV;
import npvs.NPVS;
import npvs.NPVSStub;


public class TransactionManagerImpl implements TransactionManager {

    private NPVS npvs;
    private KeyValueDriver driver;
    private Certifier certifier;


    public TransactionManagerImpl() {
        npvs = new NPVSStub(0,0);
        driver = new MongoKV("mongodb://127.0.0.1:27017", "lei", "teste");
        certifier = new CertifierStub("localhost:6000");
    }

    @Override
    public Transaction startTransaction() {

        Timestamp ts = certifier.start();
        return new TransactionImpl(npvs, driver, ts);
    }

    @Override
    public void tryCommit(Transaction tx) {

        Timestamp tc = certifier.commit(tx.getWriteSet(), tx.getStartTimestamp());
        if(tc != null) {
            tx.flush(tc);
            certifier.update();
        } else {
            System.out.println("abort");
        }
    }
}
