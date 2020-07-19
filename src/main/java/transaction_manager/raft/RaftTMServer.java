package transaction_manager.raft;

import certifier.Timestamp;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import nosql.KeyValueDriver;
import npvs.NPVS;
import org.apache.commons.io.FileUtils;
import transaction_manager.messaging.*;
import transaction_manager.raft.rpc.RequestProcessor;
import transaction_manager.raft.rpc.ValueResponse;
import transaction_manager.raft.snapshot.ExtendedState;

import java.io.File;
import java.io.IOException;

public class RaftTMServer {
    private RaftGroupService raftGroupService;
    private Node node;
    private ManagerStateMachine fsm;
    private NPVS<Long> npvs;
    private KeyValueDriver driver;
    private ServersContextMessage scm;
    private long timestep;
    private String dataPath;
    private String groupId;
    private PeerId serverId;
    private NodeOptions nodeOptions;

    public void start() throws IOException {
        // Initialize the path.
        FileUtils.forceMkdir(new File(dataPath));

        // Here Raft RPC and business RPC share the same RPC server. They can use different RPC servers, too.
        final RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());
        // Register the business processor.

        RequestHandler requestHandler = new RequestHandler(this);

        rpcServer.registerProcessor(new RequestProcessor<TransactionCommitRequest, Timestamp<Long>>(
                TransactionCommitRequest.class,
                (req , closure) -> requestHandler.tryCommit(req.getTransactionContentMessage(), closure)
        ));

        rpcServer.registerProcessor(new RequestProcessor<TransactionStartRequest, Timestamp<Long>>(
                TransactionStartRequest.class,
                (req , closure) -> requestHandler.startTransaction(closure))
        );

        rpcServer.registerProcessor(new RequestProcessor<ServerContextRequestMessage, ServersContextMessage>(
                ServerContextRequestMessage.class,
                (req , closure) -> requestHandler.getServersContext(closure)
        ));

        rpcServer.registerProcessor(new RequestProcessor<GetFullState, ExtendedState>(
                GetFullState.class,
                (req, closure) -> {closure.success(fsm.getExtendedState()); closure.run(Status.OK());}
        ));

        rpcServer.registerProcessor(new RequestProcessor<GetTimestamp, Timestamp<Long>>(
                GetTimestamp.class,
                (req, closure) -> {closure.success(fsm.getCurrentTs()); closure.run(Status.OK());}
        ));


        // Initialize the state machine.
        this.fsm = new ManagerStateMachine(timestep, npvs, driver, scm, requestHandler);
        // Set the state machine to the startup parameters.
        nodeOptions.setFsm(this.fsm);
        // Set the storage path.
        // Required. Specify the log.
        nodeOptions.setLogUri(dataPath + File.separator + "log");
        // Required. Specify the metadata.
        nodeOptions.setRaftMetaUri(dataPath + File.separator + "raft_meta");
        // Recommended. Specify the snapshot.
        nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot");
        // Initialize the Raft group service framework.
        this.raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions, rpcServer);
        // Startup
        this.node = this.raftGroupService.start();
    }

    public ManagerStateMachine getFsm() {
        return this.fsm;
    }

    public Node getNode() {
        return this.node;
    }

    public RaftGroupService RaftGroupService() {
        return this.raftGroupService;
    }

    /**
     * Redirect request to new leader
     */
    public ValueResponse<Void> redirect() {
        System.out.println("Redirectingggggg");
        final ValueResponse<Void> response = new ValueResponse<>();
        response.setSuccess(false);
        if (this.node != null) {
            final PeerId leader = this.node.getLeaderId();
            if (leader != null) {
                response.setRedirect(leader.toString());
            }
        }
        return response;
    }

    public void setNpvs(NPVS<Long> npvs) {
        this.npvs = npvs;
    }

    public void setDriver(KeyValueDriver driver) {
        this.driver = driver;
    }

    public void setScm(ServersContextMessage scm) {
        this.scm = scm;
    }

    public void setTimestep(long timestep) {
        this.timestep = timestep;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public void setServerId(PeerId serverId) {
        this.serverId = serverId;
    }

    public void setNodeOptions(NodeOptions nodeOptions) {
        this.nodeOptions = nodeOptions;
    }

    public static void main(final String[] args) throws IOException {
        if (args.length != 5) {
            System.out
                    .println("Usage: java com.alipay.sofa.jraft.example.counter.CounterServer {dataPath} {groupId} {serverId} {initConf} {offset}");
            System.out
                    .println("Example: java com.alipay.sofa.jraft.example.counter.CounterServer /tmp/server1 counter 127.0.0.1:8081 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083 1");
            System.exit(1);
        }
        final String dataPath = args[0];
        final String groupId = args[1];
        final String serverIdStr = args[2];
        final String initConfStr = args[3];
        final int offset = Integer.parseInt(args[4]);

        new RaftServerBuilder()
            .withStandardConf(initConfStr)
            .withRaftServerId(serverIdStr)
            .withRaftDataPath(dataPath)
            .withRaftGroupId(groupId)
            .withTimestep(1000)
            .withStandardServersPort(offset, 2)
            .withDatabaseCollectionName("teste1")
            .withDatabaseName("testeLei")
            .withDatabaseURI("mongodb://127.0.0.1:27017")
            .build().start();
    }
}
