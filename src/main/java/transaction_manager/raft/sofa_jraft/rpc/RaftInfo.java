package transaction_manager.raft.sofa_jraft.rpc;

import com.alipay.sofa.jraft.entity.PeerId;

public class RaftInfo<V> {
    V response;
    PeerId leader;
    boolean leaderChange = false;

    public RaftInfo(V response){
        this.response = response;
    }

    public RaftInfo(V response, PeerId leader){
        this(response);
        this.leader = leader;
        leaderChange = true;
    }

    public boolean isLeaderChange() {
        return leaderChange;
    }

    public V getResponse() {
        return response;
    }

    public PeerId getLeader() {
        return leader;
    }
}
