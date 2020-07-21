package transaction_manager.raft.sofa_jraft;

import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.alipay.sofa.jraft.util.Endpoint;
import transaction_manager.State;
import transaction_manager.TransactionManager;
import transaction_manager.messaging.*;
import transaction_manager.raft.sofa_jraft.rpc.RaftInfo;
import transaction_manager.raft.sofa_jraft.rpc.ValueResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static transaction_manager.raft.sofa_jraft.rpc.RaftMessagingService.*;

public class RaftTransactionManagerStub implements TransactionManager {
    private final CliClientServiceImpl cliClientService;
    private final String groupId;
    private PeerId leader;
    private Configuration conf;


    public RaftTransactionManagerStub(String groupId, String confStr){
        this.groupId = groupId;
        this.conf = new Configuration();
        if (!conf.parse(confStr)) {
            throw new IllegalArgumentException("Fail to parse conf:" + confStr);
        }
        cliClientService = new CliClientServiceImpl();
        cliClientService.init(new CliOptions());
        try {
            leader = refreshLeader(cliClientService, groupId, conf);
        } catch (TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }


    @SuppressWarnings("unchecked")
    @Override
    public CompletableFuture<Timestamp<Long>> startTransaction() {
        try {
            RaftInfo<Timestamp<Long>> ri = getResponseFromLeader(cliClientService, new TransactionStartRequest(), leader);
            if(ri.isLeaderChange())
                leader = ri.getLeader();
            return CompletableFuture.completedFuture(ri.getResponse());
        } catch (RemotingException | InterruptedException e) {
            e.printStackTrace();
        }
        return CompletableFuture.completedFuture(new MonotonicTimestamp(-1L));
    }

    @SuppressWarnings("unchecked")
    @Override
    public CompletableFuture<Timestamp<Long>> tryCommit(TransactionContentMessage tx) {
        try {
            RaftInfo<Timestamp<Long>> ri = getResponseFromLeader(cliClientService, new TransactionCommitRequest(tx), leader);
            if(ri.isLeaderChange())
                leader = ri.getLeader();
            return CompletableFuture.completedFuture(ri.getResponse());
        } catch (InterruptedException | RemotingException e) {
            e.printStackTrace();
        }
        return CompletableFuture.completedFuture(new MonotonicTimestamp(-1));
    }


    @SuppressWarnings("unchecked")
    @Override
    public ServersContextMessage getServersContext() {
        try {
            leader = refreshLeader(cliClientService, groupId, conf);
            return getResponseFromFollower(cliClientService, new ServerContextRequestMessage(), leader);
        } catch (InterruptedException | RemotingException | TimeoutException e) {
            e.printStackTrace();
        }
        return null;
    }

    //debug
    @SuppressWarnings("unchecked")
    public State getExtendedState(int index) {
        try {
            RaftInfo<State> ri = getResponseFromLeader(cliClientService, new GetFullState(), leader);
            if(ri.isLeaderChange())
                leader = ri.getLeader();
            return ri.getResponse();
        } catch (InterruptedException | RemotingException e) {
            e.printStackTrace();
        }
        return null;
    }
}
