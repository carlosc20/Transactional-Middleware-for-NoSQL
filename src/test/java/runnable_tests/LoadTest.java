package runnable_tests;

import io.atomix.utils.net.Address;
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

public class LoadTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        testParallel(30000);
        //testSequential(30000);
    }

    final static int KEY_POOL = 100;
    final static int CLIENTS = 2;
    final static int TRANSACTIONS = 10; // sequential -> dividido pelos clients, parallel -> por client
    final static int WRITES = 2;
    final static int READS = 2;


    static void read(Transaction tx, Random rnd) {
        String key = String.valueOf(rnd.nextInt(KEY_POOL));
        tx.read(key.getBytes());
    }

    static void write(Transaction tx, Random rnd) {
        String key = String.valueOf(rnd.nextInt(KEY_POOL));
        String value = String.valueOf(rnd.nextInt());
        tx.write(key.getBytes(), value.getBytes());
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
            timer.addCheckpoint("Transaction started " + i);


            for (int r = 0, w = 0; r + w < READS + WRITES;) {
                if(r >= READS ) {
                    write(tx, rnd);
                    timer.addCheckpoint("Write " + i);
                    w++;
                } else  if(w >= WRITES) {
                    read(tx, rnd);
                    timer.addCheckpoint("Read " + i);
                    r++;
                } else {
                    if (rnd.nextBoolean()) {
                        read(tx, rnd);
                        timer.addCheckpoint("Read " + i);
                        r++;
                    } else {
                        write(tx, rnd);
                        timer.addCheckpoint("Write " + i);
                        w++;
                    }
                }
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
            t.printStats();
        }
    }

}
