package runnable_tests;

import io.atomix.utils.net.Address;
import nosql.KeyValueDriver;
import nosql.MongoAsynchKV;
import npvs.NPVSServer;
import npvs.NPVSStub;
import spread.SpreadException;
import transaction_manager.messaging.ServersContextMessage;
import transaction_manager.standalone.TransactionManagerServer;

import java.net.UnknownHostException;
import java.util.ArrayList;

public class Servers {

    public static void main(String[] args) throws SpreadException, UnknownHostException {
        ArrayList<String> npvsServers = new ArrayList<>();
        npvsServers.add("localhost:20000");
        npvsServers.add("localhost:20001");

        new NPVSServer(20000).start();
        new NPVSServer(20001).start();
        System.out.println("NPVS servers ready");

        int serverPort = 30000;
        long timestep = 1000;
        int npvsStubPort = 30001;
        String databaseURI = "mongodb://127.0.0.1:27017";
        String databaseName =  "testeLei";
        String databaseCollectionName = "teste1";
        NPVSStub npvs = new NPVSStub(Address.from(npvsStubPort), npvsServers);
        KeyValueDriver driver = new MongoAsynchKV(databaseURI, databaseName, databaseCollectionName);
        ServersContextMessage scm = new ServersContextMessage(databaseURI, databaseName, databaseCollectionName, npvsServers);
        new TransactionManagerServer(timestep, serverPort, npvs, driver, scm).start();
        System.out.println("Transaction Manager Server ready");
    }
}
