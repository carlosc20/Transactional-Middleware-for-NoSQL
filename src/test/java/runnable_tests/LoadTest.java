package runnable_tests;

import io.atomix.utils.net.Address;
import transaction_manager.OperationFailedException;
import transaction_manager.Transaction;
import transaction_manager.TransactionController;
import transaction_manager.TransactionManager;
import transaction_manager.raft.sofa_jraft.RaftTransactionManagerStub;
import transaction_manager.standalone.TransactionManagerStub;
import utils.timer.Timer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LoadTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        testParallelSingle(30000);
        //testSequential(30000);
    }

    final static int KEY_POOL = 100;
    final static int CLIENTS = 2;
    final static int TRANSACTIONS = 100; // sequential -> dividido pelos clients, parallel -> por client
    final static int WRITES = 2;
    final static int READS = 2;


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

    static void testSequential(int serverPort) throws ExecutionException, InterruptedException {

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



}
