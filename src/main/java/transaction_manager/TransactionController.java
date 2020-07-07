package transaction_manager;

import certifier.Timestamp;
import nosql.KeyValueDriver;
import nosql.MongoAsynchKV;
import npvs.NPVS;
import npvs.NPVSStub;
import transaction_manager.messaging.ServersContextMessage;

public class TransactionController {
    private NPVS<Long> npvs;
    private KeyValueDriver driver;
    private final TransactionManager serverStub;
    private final int myPort;

    public TransactionController(int myPort, int serverPort){
        this.serverStub = new TransactionManagerStub(myPort, serverPort);
        this.myPort = myPort;
    }

    public void buildContext(){
        ServersContextMessage scm = serverStub.getServersContext();
        this.npvs = new NPVSStub(myPort, scm.getNpvsPort());
        this.driver = new MongoAsynchKV(scm.getDatabaseURI(), scm.getDatabaseName(), scm.getDatabaseCollectionName());
    }

    public Transaction startTransaction(){
        Timestamp<Long> ts = serverStub.startTransaction();
        return new TransactionImpl(npvs, driver, serverStub, ts);
    }
}
