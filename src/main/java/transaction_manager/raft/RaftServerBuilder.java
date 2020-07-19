package transaction_manager.raft;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import io.atomix.utils.net.Address;
import nosql.KeyValueDriver;
import nosql.MongoAsynchKV;
import npvs.NPVS;
import npvs.NPVSStub;
import transaction_manager.messaging.ServersContextMessage;

import java.io.IOException;
import java.util.ArrayList;

public class RaftServerBuilder {
    private long timestep;
    private Address npvsStubPort;
    private ArrayList<String> npvsServers;
    private String databaseURI;
    private String databaseName;
    private String databaseCollectionName;
    private String dataPath;
    private String groupId;
    private String serverIdStr;
    private String initConfStr;
    private NodeOptions nodeOptions;
    private String type;

    public RaftServerBuilder(){
        npvsServers =  new ArrayList<>();
    }

    public RaftTMServer build() throws IOException {
        RaftTMServer server = new RaftTMServer();
        server.setTimestep(timestep);
        server.setScm(buildServersContextMessage());
        server.setDataPath(dataPath);
        server.setGroupId(groupId);
        server.setNodeOptions(buildStandardNodeOptions());
        server.setServerId(buildPeerId());
        server.setDriver(buildDriver());
        server.setNpvs(buildNPVS());
        return server;
    }

    //use type
    private KeyValueDriver buildDriver(){
        return new MongoAsynchKV(databaseURI, databaseName, databaseCollectionName);
    }

    private NPVS<Long> buildNPVS(){
        return new NPVSStub(npvsStubPort, npvsServers);
    }

    private ServersContextMessage buildServersContextMessage(){
        return new ServersContextMessage(databaseURI, databaseName, databaseCollectionName, npvsServers);
    }

    private PeerId buildPeerId(){
        PeerId serverId = new PeerId();
        if (!serverId.parse(serverIdStr)) {
            throw new IllegalArgumentException("Fail to parse serverId:" + serverIdStr);
        }
        return serverId;
    }

    private NodeOptions buildStandardNodeOptions(){
        final NodeOptions nodeOptions = new NodeOptions();
        // For testing, adjust the snapshot interval and other parameters
        // Set the election timeout to 1 second
        nodeOptions.setElectionTimeoutMs(1000);
        // Close the CLI service.
        nodeOptions.setDisableCli(false);
        // Snapshot every 10 min
        nodeOptions.setSnapshotIntervalSecs(60 * 10);
        final Configuration initConf = new Configuration();
        if (!initConf.parse(initConfStr)) {
            throw new IllegalArgumentException("Fail to parse initConf:" + initConfStr);
        }
        nodeOptions.setInitialConf(initConf);
        return nodeOptions;
    }

    public RaftServerBuilder withType(String type){
        this.type = type;
        return this;
    }

    public RaftServerBuilder withStandardServersPort(int offset, int numberOfNPVS){
        npvsStubPort = Address.from(30000 + offset);
        for (int i = 0; i < numberOfNPVS; i++){
            int port = 20000 + i;
            npvsServers.add("localhost:" + port);
        }
        return this;
    }

    public RaftServerBuilder withStandardConf(String initConfStr){
        this.initConfStr = initConfStr;
        return this;
    }

    public RaftServerBuilder withRaftServerId(String serverIdStr){
        this.serverIdStr = serverIdStr;
        return this;
    }

    public RaftServerBuilder withRaftGroupId(String groupId){
        this.groupId = groupId;
        return this;
    }

    public RaftServerBuilder withRaftDataPath(String dataPath){
        this.dataPath = dataPath;
        return this;
    }

    public RaftServerBuilder withDatabaseCollectionName(String databaseCollectionName){
        this.databaseCollectionName = databaseCollectionName;
        return this;
    }

    public RaftServerBuilder withDatabaseURI(String databaseURI){
        this.databaseURI = databaseURI;
        return this;
    }

    public RaftServerBuilder withDatabaseName(String databaseName){
        this.databaseName = databaseName;
        return this;
    }

    public RaftServerBuilder withTimestep(long timestep){
        this.timestep = timestep;
        return this;
    }

    public RaftServerBuilder npvsStubPort(Address npvsStubPort){
        this.npvsStubPort = npvsStubPort;
        return this;
    }

    public RaftServerBuilder addNpvsServer(String npvsServerAddress){
        this.npvsServers.add(npvsServerAddress);
        return this;
    }
}
