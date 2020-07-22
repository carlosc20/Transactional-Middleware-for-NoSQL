package runnable_tests;

import io.atomix.utils.net.Address;
import nosql.KeyValueDriver;
import nosql.MongoAsynchKV;
import transaction_manager.OperationFailedException;
import transaction_manager.Transaction;
import transaction_manager.TransactionController;
import transaction_manager.TransactionManager;
import transaction_manager.raft.sofa_jraft.RaftTransactionManagerStub;
import transaction_manager.standalone.TransactionManagerStub;
import transaction_manager.utils.ByteArrayWrapper;
import utils.timer.Timer;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LoadTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //testParallel(30000);
        testMultiRaft(30000);
    }

    final static int KEY_POOL = 100;
    final static int CLIENTS = 2;
    final static int TRANSACTIONS = 100; // sequential -> dividido pelos clients, parallel -> por client
    final static int WRITES = 100;
    final static int READS = 0;


    static void read(Transaction tx, Random rnd) {
        String key = String.valueOf(rnd.nextInt(KEY_POOL));
        try {
            tx.read(key.getBytes());
        } catch (OperationFailedException e) {
            e.printStackTrace();
        }
    }

    static void write(Transaction tx, Random rnd) {
        String key = String.valueOf(rnd.nextInt(KEY_POOL));
        String value = String.valueOf(rnd.nextInt());
        tx.write(key.getBytes(), value.getBytes());
    }

    static void testParallelSingle(int serverPort) throws ExecutionException, InterruptedException {

        ExecutorService pool = Executors.newFixedThreadPool(4);

        Random rnd = new Random(0);

        Timer timer = new Timer();
        timer.start();

        TransactionManager tms = new RaftTransactionManagerStub( "manager", "127.0.0.1:8081,127.0.0.1:8082");;
        TransactionController tc = new TransactionController(Address.from(60000), tms);
        tc.buildContext();

        timer.addCheckpoint("start");
        for (int j = 0; j < TRANSACTIONS; j++) {
            pool.execute(() -> {
                try {
                    Transaction tx = tc.startTransaction();

                    for (int r = 0, w = 0; r + w < READS + WRITES;) {
                        if(r >= READS ) {
                            write(tx, rnd);
                            w++;
                        } else  if(w >= WRITES) {
                            read(tx, rnd);
                            r++;
                        } else {
                            if (rnd.nextBoolean()) {
                                read(tx, rnd);
                                r++;
                            } else {
                                write(tx, rnd);
                                w++;
                            }
                        }
                    }

                    if(!tx.commit()) {
                        System.out.println("Conflict");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        pool.awaitTermination(60, TimeUnit.SECONDS);

        timer.addCheckpoint("end");
        timer.print();
    }

    static void testParallel(int serverPort) throws ExecutionException, InterruptedException {

        List<Timer> timers = new ArrayList<>(CLIENTS);

        List<Thread> threads = new ArrayList<>(CLIENTS);
        Random rnd = new Random();
        for (int i = 0; i < CLIENTS; i++) {
            int p = i;
            threads.add(new Thread(() ->
                    {
                        Timer timer = new Timer();
                        timers.add(timer);
                        timer.start();

                        TransactionManager tms = new RaftTransactionManagerStub( "manager", "127.0.0.1:8081,127.0.0.1:8082");;
                        TransactionController tc = new TransactionController(Address.from(60000 + p), tms);
                        tc.buildContext();

                        timer.addCheckpoint(p + " -> Controllers created with context");
                        for (int j = 0; j < TRANSACTIONS; j++) {

                            try {
                                Transaction tx = tc.startTransaction();
                                timer.addCheckpoint(p + " -> Transaction started " + j, "Start");
                                System.out.println(p + " -> Transaction started " + j);

                                for (int r = 0, w = 0; r + w < READS + WRITES;) {
                                    if(r >= READS ) {
                                        write(tx, rnd);
                                        timer.addCheckpoint(p + " -> Write " + j, "Write");
                                        w++;
                                    } else  if(w >= WRITES) {
                                        read(tx, rnd);
                                        timer.addCheckpoint(p + " -> Read " + j, "Read");
                                        r++;
                                    } else {
                                        if (rnd.nextBoolean()) {
                                            read(tx, rnd);
                                            timer.addCheckpoint(p + " -> Read " + j, "Read");
                                            r++;
                                        } else {
                                            write(tx, rnd);
                                            timer.addCheckpoint(p + " -> Write " + j, "Write");
                                            w++;
                                        }
                                    }
                                }

                                if(!tx.commit()) {
                                    System.out.println("Conflict");
                                }
                                timer.addCheckpoint(p + " -> Transaction commited " + j, "Commit");
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

        for (Timer t : timers) {
            t.print();
        }
    }

    static void testSequentialStandaloneMultiClient(int serverPort) throws ExecutionException, InterruptedException {

        Timer timer = new Timer();
        timer.start();


        List<TransactionController> controllers = new ArrayList<>();
        for (int i = 0; i < CLIENTS; i++) {
            TransactionManager tms = new TransactionManagerStub(50000 + i, serverPort);
            TransactionController tc = new TransactionController(Address.from(60000 + i), tms);
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
            timer.addCheckpoint("Transaction started " + i, "start");


            for (int r = 0, w = 0; r + w < READS + WRITES;) {
                if(r >= READS ) {
                    write(tx, rnd);
                    timer.addCheckpoint("Write " + i, "write");
                    w++;
                } else  if(w >= WRITES) {
                    read(tx, rnd);
                    timer.addCheckpoint("Read " + i, "read");
                    r++;
                } else {
                    if (rnd.nextBoolean()) {
                        read(tx, rnd);
                        timer.addCheckpoint("Read " + i, "read");
                        r++;
                    } else {
                        write(tx, rnd);
                        timer.addCheckpoint("Write " + i, "write");
                        w++;
                    }
                }
            }

            if(!tx.commit()) {
                conflicts++;
            }
            timer.addCheckpoint("Transaction commited " + i, "commit");
        }

        System.out.println("Conflicts: " + conflicts);

        timer.print();
    }

    static void testMultiBD(int serverPort) throws ExecutionException, InterruptedException {

        String databaseURI = "mongodb://127.0.0.1:27017";
        String databaseName =  "testeLei";
        String databaseCollectionName = "teste1";
        KeyValueDriver db= new MongoAsynchKV(databaseURI, databaseName, databaseCollectionName);

        Timer timer = new Timer();
        timer.start();
        timer.addCheckpoint("Start");

        ExecutorService pool = Executors.newFixedThreadPool(4);

        Random rnd = new Random();
        for (int i = 0; i < TRANSACTIONS; i++) {
            pool.execute(() -> {



                for (int j = 0; j < READS; j++) {
                    String key = String.valueOf(rnd.nextInt(KEY_POOL));
                    db.get(new ByteArrayWrapper(key.getBytes()));
                }

                Map<ByteArrayWrapper, byte[]> writeMap;
                if (WRITES > 0) {
                    writeMap = new HashMap<>();
                    for (int j = 0; j < WRITES; j++) {
                        String key = String.valueOf(rnd.nextInt(KEY_POOL));
                        String value = String.valueOf(rnd.nextInt(KEY_POOL));
                        writeMap.put(new ByteArrayWrapper(key.getBytes()), value.getBytes());
                    }
                    db.put(writeMap);
                }

            });
        }

        pool.shutdown();
        pool.awaitTermination(60, TimeUnit.SECONDS);
        timer.addCheckpoint("End");
        timer.print();
    }

    static void testMultiStandalone(int serverPort) throws ExecutionException, InterruptedException {

        Timer timer = new Timer();
        timer.start();
        timer.addCheckpoint("Start");


        TransactionManager tms = new TransactionManagerStub(50000, serverPort);
        TransactionController tc = new TransactionController(Address.from(60000), tms);
        tc.buildContext();

        System.out.println("Controllers ready");
        timer.addCheckpoint("Controllers created with context");

        ExecutorService pool = Executors.newFixedThreadPool(4);

        Random rnd = new Random();
        for (int i = 0; i < TRANSACTIONS; i++) {
            pool.execute(() -> {
                try {
                    Transaction tx = tc.startTransaction();
                    for (int j = 0; j < READS; j++) {
                        read(tx,rnd);
                    }
                    if (WRITES > 0) {
                        for (int j = 0; j < WRITES; j++) {
                            write(tx,rnd);
                        }
                    }
                    tx.commit();
                    System.out.println("commited");
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }

        pool.shutdown();
        pool.awaitTermination(60, TimeUnit.SECONDS);
        timer.addCheckpoint("End");
        timer.print();
    }

    /*
    for (int i = 0; i < TRANSACTIONS; i++) {
            int t = i;
            pool.execute(() -> {
                try {
                    Transaction tx = tc.startTransaction();
                    timer.addCheckpoint("start " + t, "start");
                    for (int j = 0; j < READS; j++) {
                        read(tx,rnd);
                    }
                    timer.addCheckpoint("reads " + t, "read");
                    if (WRITES > 0) {
                        for (int j = 0; j < WRITES; j++) {
                            write(tx,rnd);
                        }
                    }
                    timer.addCheckpoint("write " + t, "write");
                    tx.commit();
                    timer.addCheckpoint("commit " + t, "commit");
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
     */

    static void testMultiRaft(int serverPort) throws ExecutionException, InterruptedException {

        Timer timer = new Timer();
        timer.start();
        timer.addCheckpoint("Start");

        TransactionManager tms = new RaftTransactionManagerStub( "manager", "127.0.0.1:8081,127.0.0.1:8082");;
        TransactionController tc = new TransactionController(Address.from(60000), tms);
        tc.buildContext();

        System.out.println("Controllers ready");
        timer.addCheckpoint("Controllers created with context");

        ExecutorService pool = Executors.newFixedThreadPool(4);

        Random rnd = new Random();
        for (int i = 0; i < TRANSACTIONS; i++) {
            pool.execute(() -> {
                try {
                    Transaction tx = tc.startTransaction();
                    for (int j = 0; j < READS; j++) {
                        read(tx,rnd);
                    }

                    if (WRITES > 0) {
                        for (int j = 0; j < WRITES; j++) {
                            write(tx,rnd);
                        }
                    }
                    tx.commit();
                    //System.out.println("commited");
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }

        pool.shutdown();
        pool.awaitTermination(60, TimeUnit.SECONDS);
        timer.addCheckpoint("End");
        timer.print();
    }
}
