package transaction_manager;

import certifier.Timestamp;
import nosql.KeyValueDriver;
import npvs.NPVS;

public class TransactionBridge {
    private NPVS<Long> npvs;
    private KeyValueDriver driver;

    //TODO tornar mais genérico com o stub
    private TransactionManager serverStub;

    public TransactionBridge(int port){
        //this.serverStub = new TransactionManagerStub(port);
    }

    public void buildContext(){

    }

    public Transaction startTransaction(){
        Timestamp<Long> ts = serverStub.startTransaction();
        return new TransactionImpl(npvs, driver, ts);
    }
}
