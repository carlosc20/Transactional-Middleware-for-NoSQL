package transaction_manager.raft.sofa_jraft.rpc;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.alipay.sofa.jraft.util.Endpoint;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

public class RaftMessagingService {
    public static PeerId refreshLeader(CliClientServiceImpl cliClientService, String groupId) throws TimeoutException, InterruptedException {
        if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 5000).isOk()) {
            int waitMs = 2000;
            Thread.sleep(waitMs);
            System.out.println("Waiting for raft, retrying in " + waitMs/1000 + " seconds");
            refreshLeader(cliClientService, groupId);
        }
        PeerId leader = RouteTable.getInstance().selectLeader(groupId);
        System.out.println("Leader is " + leader);
        return leader;
    }


    @SuppressWarnings("unchecked")
    public static <Q,R> RaftInfo<R> getResponseFromLeader(CliClientServiceImpl cliClientService, Q query, PeerId leader) throws RemotingException, InterruptedException {
        ValueResponse<R> vr = (ValueResponse<R>) cliClientService.getRpcClient().invokeSync(leader.getEndpoint(), query, 50000);
        if(!vr.isSuccess() && vr.getRedirect() != null) {
            Endpoint e = JRaftUtils.getEndPoint(vr.getRedirect());
            leader = JRaftUtils.getPeerId(vr.getRedirect());
            return new RaftInfo<>(((ValueResponse<R>) cliClientService.getRpcClient().invokeSync(e, query, 50000)).getValue(), leader);
        }
        else
            return new RaftInfo<>(vr.getValue());
    }

    //must perform a refresh before using it
    @SuppressWarnings("unchecked")
    public static  <Q,R> R getResponseFromFollower(CliClientServiceImpl cliClientService, Q query, PeerId leader) throws RemotingException, InterruptedException {
        List<PeerId> pps = RouteTable.getInstance().getConfiguration("manager").getPeers();
        pps.remove(leader);
        int randomIndex = new Random().nextInt(pps.size());
        PeerId pp = pps.get(randomIndex);
        return ((ValueResponse<R>) cliClientService.getRpcClient().invokeSync(pp.getEndpoint(), query, 50000)).getValue();
    }


}
