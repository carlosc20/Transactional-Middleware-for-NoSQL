package transaction_manager.messaging;

public class ServersContextMessage {
    private final String databaseURI;
    private final String databaseName;
    private final String databaseCollectionName;
    private final int npvsPort;

    public ServersContextMessage(String databaseURI, String databaseName, String databaseCollectionName, int npvsPort){
        this.databaseURI = databaseURI;
        this.databaseName = databaseName;
        this.databaseCollectionName = databaseCollectionName;
        this.npvsPort = npvsPort;
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

    public int getNpvsPort() {
        return npvsPort;
    }
}
