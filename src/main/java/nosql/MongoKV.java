package nosql;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;

public class MongoKV implements KeyValueDriver {

    private MongoCollection<Document> collection;

    public MongoKV(String uri) {
        MongoClient mongoClient = MongoClients.create(uri);

        MongoDatabase database = mongoClient.getDatabase("TheDatabaseName");
        collection = database.getCollection("TheCollectionName");
    }

    //TODO usar _id em vez de key?
    // https://mongodb.github.io/mongo-java-driver/3.12/driver/

    @Override
    public byte[] read(byte[] key) {
        Document doc = collection.find(eq("key", key)).first();
        System.out.println(doc.toJson());
        Object value = doc.get(new String(key));
        return new byte[0];
    }

    @Override
    public void write(byte[] key, byte[] value) {
        Document doc = new Document(new String(key), new String(value));
        collection.insertOne(doc); // updateOne ?
        // collection.insertMany(documents); List<Document>
    }

    @Override
    public void delete(byte[] key) {
        collection.deleteOne(eq("key", key));
    }
}