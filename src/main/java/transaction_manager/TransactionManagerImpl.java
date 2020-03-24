package transaction_manager;

import certifier.Certifier;
import certifier.CertifierStub;
import certifier.Timestamp;
import nosql.KeyValueDriver;
import nosql.MongoKV;
import npvs.NPVS;
import npvs.NPVSImpl;


public class TransactionManagerImpl implements TransactionManager {

    private NPVS npvs;
    private KeyValueDriver driver;

    private Certifier certifier;


    public TransactionManagerImpl() {
        npvs = new NPVSImpl();
        driver = new MongoKV("mongodb://hostOne:27017");
        certifier = new CertifierStub("localhost:6000");
    }

    @Override
    public Transaction startTransaction() {
        Timestamp ts = certifier.start();

        return new TransactionImpl(npvs, driver, ts);
    }

    @Override
    public void tryCommit(Transaction tx) {

        Timestamp ts = certifier.commit(tx.getWriteSet());
        if(ts == null)
            System.out.println("erro");
        tx.flush();
        certifier.update();
    }
}
