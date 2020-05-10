package jraft;

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.alipay.sofa.jraft.error.RemotingException;
import jraft.rpc.PutRequest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

public class CertifierClient {

    public static void main(final String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Useage : java com.alipay.sofa.jraft.example.counter.CounterClient {groupId} {conf}");
            System.out
                    .println("Example: java com.alipay.sofa.jraft.example.counter.CounterClient counter 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
            System.exit(1);
        }
        final String groupId = args[0];
        final String confStr = args[1];

        final Configuration conf = new Configuration();
        if (!conf.parse(confStr)) {
            throw new IllegalArgumentException("Fail to parse conf:" + confStr);
        }

        RouteTable.getInstance().updateConfiguration(groupId, conf);

        final CliClientServiceImpl cliClientService = new CliClientServiceImpl();
        cliClientService.init(new CliOptions());

        if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 1000).isOk()) {
            throw new IllegalStateException("Refresh leader failed");
        }

        final PeerId leader = RouteTable.getInstance().selectLeader(groupId);
        System.out.println("Leader is " + leader);
        final int n = 10000;
        final CountDownLatch latch = new CountDownLatch(n);
        final long start = System.currentTimeMillis();
        //put(cliClientService, leader, 1, 10);
       for (int i = 0; i < n; i++) {
            incrementAndGet(cliClientService, leader, 1, latch);
        }
       
        latch.await();

        System.out.println(n + " ops, cost : " + (System.currentTimeMillis() - start) + " ms.");
        System.exit(0);
    }

    private static void incrementAndGet(final CliClientServiceImpl cliClientService, final PeerId leader,
                                        final long delta, CountDownLatch latch) throws RemotingException,
            InterruptedException {
        final IncrementAndGetRequest request = new IncrementAndGetRequest();
        request.setDelta(delta);
        cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {

            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    latch.countDown();
                    System.out.println("incrementAndGet result:" + result);
                } else {
                    err.printStackTrace();
                    latch.countDown();
                }
            }

            @Override
            public Executor executor() {
                return null;
            }
        }, 30000);
    }

    private static void put(final CliClientServiceImpl cliClientService, final PeerId leader,
                                        final long value, final long key) throws RemotingException,
            InterruptedException {
        final PutRequest request = new PutRequest();
        request.setKey(key);
        request.setValue(value);
        cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {

            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    System.out.println("Put result:" + result);
                } else {
                    err.printStackTrace();
                }
            }

            @Override
            public Executor executor() {
                return null;
            }
        }, 5000);
    }

}