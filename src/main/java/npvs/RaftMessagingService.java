package npvs;

import certifier.Timestamp;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import transaction_manager.messaging.*;
import transaction_manager.raft.sofa_jraft.rpc.ValueResponse;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

public class RaftMessagingService {

    private final CliClientServiceImpl cliClientService;
    private final String groupId;
    private PeerId leader;


    public RaftMessagingService(String groupId, String confStr){
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

    // raft servers should be ready first
    public void refreshLeader() throws TimeoutException, InterruptedException {
        if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 1000).isOk()) {
            int waitMs = 2000;
            Thread.sleep(waitMs);
            System.out.println("Waiting for raft, retrying in " + waitMs/1000 + " seconds");
            refreshLeader();
        }
        leader = RouteTable.getInstance().selectLeader(groupId);
        System.out.println("Leader is " + leader);
    }

    @SuppressWarnings("unchecked")
    public Timestamp<Long> getTimestamp() {
        try {
            List<PeerId> pps = RouteTable.getInstance().getConfiguration("manager").getPeers();
            pps.remove(leader);
            int randomIndex = new Random().nextInt(pps.size());
            PeerId pp = pps.get(randomIndex);

            return ((ValueResponse<Timestamp<Long>>) cliClientService.getRpcClient().invokeSync(pp.getEndpoint(), new GetTimestamp(), 30000)).getValue();
        } catch (InterruptedException | RemotingException e) {
            e.printStackTrace();
        }
        return null;
    }
}
