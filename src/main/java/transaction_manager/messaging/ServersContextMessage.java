package transaction_manager.messaging;

import io.atomix.utils.net.Address;

import java.util.List;

public class ServersContextMessage {
    private final String databaseURI;
    private final String databaseName;
    private final String databaseCollectionName;
    private final List<Address> npvsServers;

    public ServersContextMessage(String databaseURI, String databaseName, String databaseCollectionName, List<Address> npvsServers){
        this.databaseURI = databaseURI;
        this.databaseName = databaseName;
        this.databaseCollectionName = databaseCollectionName;
        this.npvsServers = npvsServers;
    }

    public String getDatabaseURI() {
        return databaseURI;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getDatabaseCollectionName() {
        return databaseCollectionName;
    }

    public List<Address> getNpvsServers() {
        return npvsServers;
    }
}
