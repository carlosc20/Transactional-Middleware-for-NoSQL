package transaction_manager;

import certifier.Certifier;
import certifier.Timestamp;
import nosql.KeyValueDriver;
import npvs.NPVS;
import npvs.NPVSImpl;


public class TransactionManagerImpl implements TransactionManager {

    private NPVS npvs;
    private KeyValueDriver driver;

    private Certifier certifier;


    public TransactionManagerImpl() {
        npvs = new NPVSImpl();
        driver = null;
        certifier = null;
    }

    @Override
    public Transaction startTransaction() {
        Timestamp ts = certifier.start();

        return new TransactionImpl(npvs, driver, ts);
    }

    @Override
    public void tryCommit(Transaction tx) {

        Timestamp ts = certifier.commit(tx);
        if(ts == null)
            System.out.println("erro");
        tx.flush();
        certifier.update();
    }
}
