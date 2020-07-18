package transaction_manager.messaging;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ServersContextMessage implements Serializable {
    private final String databaseURI;
    private final String databaseName;
    private final String databaseCollectionName;
    private final ArrayList<String> npvsServers;

    public ServersContextMessage(String databaseURI, String databaseName, String databaseCollectionName, ArrayList<String> npvsServers){
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

    public List<String> getNpvsServers() {
        return npvsServers;
    }

    @Override
    public String toString() {
        return "ServersContextMessage{" +
                "databaseURI='" + databaseURI + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", databaseCollectionName='" + databaseCollectionName + '\'' +
                ", npvsServers=" + npvsServers.toString() +
                '}';
    }
}
