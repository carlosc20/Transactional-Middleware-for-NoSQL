import transaction_manager.client_side.Transaction;
import transaction_manager.client_side.TransactionController;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class LoadTest {

    public static void main(String[] args) {
        test();
    }

    static void test(){
        final int CLIENTS = 3;
        final int SERVER_PORT = 30000;
        final int TRANSACTIONS = 10;
        final int OPERATIONS = 3;
        final float RWP = 0.5f;

        List<TransactionController> controllers = new ArrayList<>();
        for (int i = 0; i < CLIENTS; i++) {
            TransactionController tc = new TransactionController(12000 + i, 13000 + i,SERVER_PORT);
            tc.buildContext();
            controllers.add(tc);
        }

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

            if(tx.commit());
                conflicts++;

        }

        System.out.println(conflicts);
    }

}
