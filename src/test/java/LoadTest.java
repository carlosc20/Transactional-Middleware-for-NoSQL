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

        List<Address> npvsServers = new ArrayList<>();
        npvsServers.add(Address.from(20000));
        npvsServers.add(Address.from(20001));
        new NPVSServer(20000, 40000, "1").start();
        new NPVSServer(20001, 40000, "2").start();
        System.out.println("NPVS servers ready");

        int serverPort = 30000;
        long timestep = 1000;
        int npvsStubPort = 30001;
        String databaseURI = "mongodb://127.0.0.1:27017";
        String databaseName =  "testeLei";
        String databaseCollectionName = "teste1";
        NPVS<Long> npvs = new NPVSStub(npvsStubPort, npvsServers);
        KeyValueDriver driver = new MongoAsynchKV(databaseURI, databaseName, databaseCollectionName);
        ServersContextMessage scm = new ServersContextMessage(databaseURI, databaseName, databaseCollectionName, npvsServers);
        new TransactionManagerServer(timestep, serverPort, npvs, driver, scm).start();
        System.out.println("Transaction Manager Server ready");

        testParallel(serverPort);
        //testSequential(serverPort);
    }

    static void testSequential(int serverPort) throws ExecutionException, InterruptedException {
        final int CLIENTS = 1;
        final int TRANSACTIONS = 20;
        final int WRITES = 2;
        final int READS = 2;
        final int KEY_POOL = 100;

        Timer timer = new Timer();
        timer.start();


        List<TransactionController> controllers = new ArrayList<>();
        for (int i = 0; i < CLIENTS; i++) {
            TransactionManager tms = new TransactionManagerStub(50000 + i, serverPort);
            TransactionController tc = new TransactionController(60000 + i, tms);
            tc.buildContext();
            controllers.add(tc);
        }
        System.out.println("Controllers ready");
        timer.addCheckpoint("Controllers created with context");

        int conflicts = 0;

        Random rnd = new Random();
        for (int i = 0; i < TRANSACTIONS; i++) {
            int n = rnd.nextInt(CLIENTS);
            TransactionController tc = controllers.get(n);
            Transaction tx = tc.startTransaction();
            timer.addCheckpoint("Transaction started " + i);

            for (int j = 0; j < READS; j++) {
                String key = String.valueOf(rnd.nextInt(KEY_POOL));
                tx.read(key.getBytes());
                timer.addCheckpoint("Read " + i);
            }

            for (int j = 0; j < WRITES; j++) {
                String key = String.valueOf(rnd.nextInt(KEY_POOL));
                String value = String.valueOf(rnd.nextInt());
                tx.write(key.getBytes(), value.getBytes());
                timer.addCheckpoint("Write " + i);
            }

            if(!tx.commit()) {
                conflicts++;
            }
            timer.addCheckpoint("Transaction commited " + i);
        }

        System.out.println("Conflicts: " + conflicts);

        timer.print();
    }

    static void testParallel(int serverPort) throws ExecutionException, InterruptedException {
        final int CLIENTS = 2;
        final int TRANSACTIONS = 5;
        final int WRITES = 2;
        final int READS = 2;
        final int KEY_POOL = 100;

        Timer timer = new Timer();
        timer.start();

        List<Thread> threads = new ArrayList<>(CLIENTS);
        Random rnd = new Random();
        for (int i = 0; i < CLIENTS; i++) {
            int p = i;
            threads.add(new Thread(() ->
                    {
                        TransactionManager tms = new TransactionManagerStub(50000 + p, serverPort);
                        TransactionController tc = new TransactionController(60000 + p, tms);
                        tc.buildContext();

                        timer.addCheckpoint(p + " -> Controllers created with context");
                        for (int j = 0; j < TRANSACTIONS; j++) {

                            try {
                                Transaction tx = tc.startTransaction();
                                timer.addCheckpoint(p + " -> Transaction started " + j);
                                System.out.println(p + " -> Transaction started " + j);

                                for (int k = 0; k < READS; k++) {
                                    String key = String.valueOf(rnd.nextInt(KEY_POOL));
                                    System.out.println(key);
                                    tx.read(key.getBytes());
                                    System.out.println("prox");
                                    timer.addCheckpoint(p + " -> Read " + j);
                                }

                                for (int k = 0; k < WRITES; k++) {
                                    String key = String.valueOf(rnd.nextInt(KEY_POOL));
                                    String value = String.valueOf(rnd.nextInt());
                                    tx.write(key.getBytes(), value.getBytes());
                                    timer.addCheckpoint(p + " -> Write " + j);
                                }

                                if(!tx.commit()) {
                                    System.out.println("Conflict");
                                }
                                timer.addCheckpoint(p + " -> Transaction commited " + j);
                                System.out.println(p + " -> Transaction commited " + j);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                        }
                    })
            );
        }

        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }
        System.out.println("Acabou");
        timer.print();
    }

}
