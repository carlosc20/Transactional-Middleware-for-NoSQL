package transaction_manager;

import certifier.Timestamp;
import io.atomix.utils.net.Address;
import nosql.KeyValueDriver;
import nosql.MongoAsynchKV;
import npvs.NPVSStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.messaging.ServersContextMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class TransactionController {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionController.class);
    private NPVSStub npvs;
    private KeyValueDriver driver;
    private final TransactionManager serverStub;
    private final Address npvsStubPort;

    public TransactionController(Address npvsStubPort, TransactionManager serverStub){
        this.serverStub = serverStub;
        this.npvsStubPort = npvsStubPort;
    }

    public void buildContext(){
        LOG.info("Sending request to build controller");
        ServersContextMessage scm = serverStub.getServersContext();
        this.npvs = new NPVSStub(npvsStubPort, scm.getNpvsServers());
        this.driver = new MongoAsynchKV(scm.getDatabaseURI(), scm.getDatabaseName(), scm.getDatabaseCollectionName());
        LOG.info("Controller built");
        List<String> handlers = new ArrayList<>();
        handlers.add("get");
        try {
            this.npvs.warmhup(handlers).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public Transaction startTransaction() throws ExecutionException, InterruptedException {
        LOG.info("Asking server for a new start timestamp");
        Timestamp<Long> ts = serverStub.startTransaction().get();
        LOG.info("Received TS: {}", ts.toPrimitive());
        return new TransactionImpl(npvs, driver, serverStub, ts);
    }
}
