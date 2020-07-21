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

import static transaction_manager.raft.sofa_jraft.rpc.RaftMessagingService.getResponseFromFollower;
import static transaction_manager.raft.sofa_jraft.rpc.RaftMessagingService.refreshLeader;

public class NPVSRaftClient {

    private final CliClientServiceImpl cliClientService;
    private PeerId leader;
    private String groupId;
    private Configuration conf;

    public NPVSRaftClient(String groupId, String confStr){
        this.conf = new Configuration();
        if (!conf.parse(confStr)) {
            throw new IllegalArgumentException("Fail to parse conf:" + confStr);
        }
        cliClientService = new CliClientServiceImpl();
        cliClientService.init(new CliOptions());
        this.groupId = groupId;
        try {
            leader = refreshLeader(cliClientService, groupId, conf);
        } catch (TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    public Timestamp<Long> getTimestamp() {
        try {
            leader = refreshLeader(cliClientService, groupId, conf);
            return getResponseFromFollower(cliClientService, new GetTimestamp(), leader);
        } catch (InterruptedException | RemotingException | TimeoutException e) {
            e.printStackTrace();
        }
        return null;
    }
}
