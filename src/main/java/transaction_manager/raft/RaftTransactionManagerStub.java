package transaction_manager.raft;

import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import transaction_manager.TransactionManager;
import transaction_manager.messaging.*;
import transaction_manager.raft.rpc.ValueResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

public class RaftTransactionManagerStub implements TransactionManager {
    private final CliClientServiceImpl cliClientService;
    private final String groupId;
    private PeerId leader;


    public RaftTransactionManagerStub(String groupId, String confStr){
        this.groupId = groupId;
        Configuration conf = new Configuration();
        if (!conf.parse(confStr)) {
            throw new IllegalArgumentException("Fail to parse conf:" + confStr);
        }
        RouteTable.getInstance().updateConfiguration(groupId, conf);
        cliClientService = new CliClientServiceImpl();
        cliClientService.init(new CliOptions());
        try {
            refreshLeader();
        } catch (TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void refreshLeader() throws TimeoutException, InterruptedException {
        if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 1000).isOk()) {
            throw new IllegalStateException("Refresh leader failed");
        }
        leader = RouteTable.getInstance().selectLeader(groupId);
        System.out.println("Leader is " + leader);
    }

    @Override
    public CompletableFuture<Timestamp<Long>> startTransaction() {
        TransactionStartRequest tsr = new TransactionStartRequest();
        try {
            return CompletableFuture.completedFuture(((ValueResponse<MonotonicTimestamp>) cliClientService.getRpcClient().invokeSync(leader.getEndpoint(), tsr, 30000)).getValue());
        } catch (InterruptedException | RemotingException e) {
            e.printStackTrace();
        }
        return CompletableFuture.completedFuture(new MonotonicTimestamp(-1L));
    }

    @Override
    public CompletableFuture<Timestamp<Long>> tryCommit(TransactionContentMessage tx) {
        TransactionCommitRequest tcr = new TransactionCommitRequest(tx);
        try {
            // TODO return timestamp
            return CompletableFuture.completedFuture(((ValueResponse<Timestamp<Long>>) cliClientService.getRpcClient().invokeSync(leader.getEndpoint(), tcr, 30000)).getValue());
        } catch (InterruptedException | RemotingException e) {
            e.printStackTrace();
        }
        return CompletableFuture.completedFuture(new MonotonicTimestamp(-1));
    }

    @Override
    public ServersContextMessage getServersContext() {
        ServerContextRequestMessage scr = new ServerContextRequestMessage();
        try {
            return ((ValueResponse<ServersContextMessage>) cliClientService.getRpcClient().invokeSync(leader.getEndpoint(), scr, 30000)).getValue();
        } catch (InterruptedException | RemotingException e) {
            e.printStackTrace();
        }
        return null;
    }
}
