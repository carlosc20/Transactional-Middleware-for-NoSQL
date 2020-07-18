package runnable_tests;

import certifier.Timestamp;
import io.atomix.utils.net.Address;
import transaction_manager.Transaction;
import transaction_manager.TransactionController;
import transaction_manager.TransactionImpl;
import transaction_manager.TransactionManager;
import transaction_manager.standalone.TransactionManagerStub;
import transaction_manager.utils.ByteArrayWrapper;
import utils.Timer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

public class ValidityTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // ligar servers antes e dar drop bd
        testParallel(30000);
        //testSequential(30000);
    }

    // TODO verificar
    static void testParallel(int serverPort) throws ExecutionException, InterruptedException {
        final int CLIENTS = 2;
        final int TRANSACTIONS = 5; // por client
        final int WRITES = 2;
        final int READS = 2;
        final int KEY_POOL = 100;

        ConcurrentNPVSImplLHM npvs = new ConcurrentNPVSImplLHM();

        List<Thread> threads = new ArrayList<>(CLIENTS);
        Random rnd = new Random();
        for (int i = 0; i < CLIENTS; i++) {
            int p = i;
            threads.add(new Thread(() ->
                    {
                        TransactionManager tms = new TransactionManagerStub(50000 + p, serverPort);
                        TransactionController tc = new TransactionController(Address.from(60000 + p), tms);
                        tc.buildContext();

                        for (int j = 0; j < TRANSACTIONS; j++) {

                            try {
                                TransactionImpl tx = tc.startTransaction();
                                Timestamp<Long> sts = tx.getTs();

                                System.out.println(p + " -> Transaction started " + j);

                                // TODO por reads e writes alternados

                                Map<ByteArrayWrapper, byte[]> writes = new HashMap<>();
                                for (int k = 0; k < WRITES; k++) {
                                    byte[] key = String.valueOf(rnd.nextInt(KEY_POOL)).getBytes();
                                    byte[] value = String.valueOf(rnd.nextInt()).getBytes();
                                    writes.put(new ByteArrayWrapper(key),value);
                                    tx.write(key, value);
                                }

                                for (int k = 0; k < READS; k++) {
                                    byte[] key = String.valueOf(rnd.nextInt(KEY_POOL)).getBytes();
                                    byte[] value = tx.read(key);
                                    byte[] value2 = npvs.get(new ByteArrayWrapper(key),sts).get();
                                    System.out.println(value == value2);
                                }

                                Timestamp<Long> cts = tx.commitTs();
                                npvs.put(writes, cts);
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

    }
}
