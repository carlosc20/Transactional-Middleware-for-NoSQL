import io.atomix.utils.net.Address;
import nosql.KeyValueDriver;
import nosql.MongoAsynchKV;
import npvs.NPVS;
import npvs.NPVSServer;
import npvs.NPVSStub;
import spread.SpreadException;
import transaction_manager.Transaction;
import transaction_manager.TransactionController;
import transaction_manager.TransactionManager;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.standalone.TransactionManagerServer;
import transaction_manager.standalone.TransactionManagerStub;
import utils.Timer;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class LoadTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException, SpreadException, UnknownHostException {
        Timer timer = new Timer();
        timer.start();
        final int serverPort = 30000;

        List<Address> npvsServers = new ArrayList<>();
        npvsServers.add(Address.from(20000));
        npvsServers.add(Address.from(20001));
        new NPVSServer(20000, 40000, "1").start();
        new NPVSServer(20001, 40000, "2").start();
        timer.addCheckpoint("NPVS servers started");

        long timestep = 1000;
        int npvsStubPort = 30001;
        String databaseURI = "mongodb://127.0.0.1:27017";
        String databaseName =  "testeLei";
        String databaseCollectionName = "teste1";
        NPVS<Long> npvs = new NPVSStub(npvsStubPort, npvsServers);
        KeyValueDriver driver = new MongoAsynchKV(databaseURI, databaseName, databaseCollectionName);
        ServersContextMessage scm = new ServersContextMessage(databaseURI, databaseName, databaseCollectionName, npvsServers);
        new TransactionManagerServer(timestep, 30000, npvs, driver, scm).start();
        timer.addCheckpoint("TM server started");

        test(serverPort);
        timer.addCheckpoint("Ended");

        timer.print();
    }

    static void test(int serverPort) throws ExecutionException, InterruptedException {
        final int CLIENTS = 3;
        final int TRANSACTIONS = 10;
        final int OPERATIONS = 3;
        final float RWP = 0.5f;

        Timer timer = new Timer();
        timer.start();

        List<TransactionController> controllers = new ArrayList<>();
        for (int i = 0; i < CLIENTS; i++) {
            TransactionManager tms = new TransactionManagerStub( 12346, serverPort);
            TransactionController tc = new TransactionController(12345, tms);
            tc.buildContext();
            controllers.add(tc);
        }
        timer.addCheckpoint("Controllers created with context");

        int conflicts = 0;

        Random rnd = new Random();
        for (int i = 0; i < TRANSACTIONS; i++) {
            int n = rnd.nextInt(CLIENTS);
            TransactionController tc = controllers.get(n);
            Transaction tx = tc.startTransaction(); // várias por cliente?

            for (int j = 0; j < OPERATIONS; j++) {
                String key = String.valueOf(rnd.nextInt(100));

                if(rnd.nextFloat() < RWP) {
                    tx.read(key.getBytes());
                } else {
                    String value = String.valueOf(rnd.nextInt());
                    tx.write(key.getBytes(), value.getBytes());
                }
            }

            if(!tx.commit()) {
                conflicts++;
            }
        }

        System.out.println(conflicts);

        timer.addCheckpoint("End");
        timer.print();
    }

}
