package runnable_tests;

import npvs.NPVSServer;
import spread.SpreadException;
import transaction_manager.raft.sofa_jraft.RaftServerBuilder;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class ServersRaft {
    public static void main(String[] args) throws SpreadException, IOException {

        startRaft("C:\\Users\\CarlosPeixotoAntunes\\Projects\\LEI-2019-20\\raft_server1",
                "manager",
                "127.0.0.1:8081",
                "127.0.0.1:8081,127.0.0.1:8082",
                1);

        startRaft("C:\\Users\\CarlosPeixotoAntunes\\Projects\\LEI-2019-20\\raft_server2",
                "manager",
                "127.0.0.1:8082",
                "127.0.0.1:8081,127.0.0.1:8082",
                2);

        new NPVSServer(20000).start();
        new NPVSServer(20001).start();
        System.out.println("NPVS servers ready");
    }

    private static void startRaft(String dataPath, String groupId, String serverIdStr, String initConfStr, int offset) throws IOException {

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
