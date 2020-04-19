package nosql;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.bson.types.Binary;
import utils.ByteArrayWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


import static com.mongodb.client.model.Filters.eq;

public class MongoKV implements KeyValueDriver {

    private MongoCollection<Document> collection;

    public MongoKV(String uri, String databaseName, String collectionName) {
        MongoClient mongoClient = MongoClients.create(uri);

        MongoDatabase database = mongoClient.getDatabase(databaseName);
        collection = database.getCollection(collectionName);
    }


    @Override
    public byte[] read(byte[] key) {
        Document doc = collection.find(eq("_id", new String(key))).first();
        if(doc != null)
            return ((Binary) doc.get("value")).getData();
        return null;
    }

    public List<byte[]> scan(List<byte[]> keyList) {
        return keyList.stream().map(this::read).collect(Collectors.toList());
    }

    @Override
    // todo haverá diferença entre inserir/apagar muitos de uma vez ou um de cada vez?
    public void update(Map<ByteArrayWrapper, byte[]> writeMap) {
        for(Map.Entry<ByteArrayWrapper, byte[]> kv : writeMap.entrySet()){
            byte[] value = kv.getValue();
            String key = kv.getKey().toString();
            if(value != null){
                Document doc = new Document("_id", key).append("value", value);
                collection.replaceOne(eq("_id", key), doc, new ReplaceOptions().upsert(true));
            }
            else
                collection.deleteOne(eq("_id", kv.getKey().toString()));
        }
    }

    public static void main(String[] args) {
        MongoKV mkv = new MongoKV("mongodb://127.0.0.1:27017", "testeLei", "teste1");
        HashMap<ByteArrayWrapper, byte[]> writeMap = new HashMap<>();
        ByteArrayWrapper baw2 = new ByteArrayWrapper("carlos".getBytes());
        writeMap.put(baw2, "castro2".getBytes());
        mkv.update(writeMap);
        ArrayList<byte[]> query= new ArrayList<>();
        query.add("carlos".getBytes());
        query.add("marco".getBytes());
        mkv.scan(query).forEach(v -> System.out.println(new String(v)));
    }
}
