package nosql;


import certifier.Timestamp;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import nosql.messaging.GetMessage;
import org.bson.Document;
import org.bson.types.Binary;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


import static com.mongodb.client.model.Filters.eq;

public class MongoKV implements KeyValueDriver {

    private final MongoCollection<Document> collection;

    public MongoKV(String uri, String databaseName, String collectionName) {
        MongoClient mongoClient = MongoClients.create(uri);

        MongoDatabase database = mongoClient.getDatabase(databaseName);
        collection = database.getCollection(collectionName);
    }

    //TODO colocar returns de acordo com a api async. estão assim apenas para não ter erros

    @Override
    public CompletableFuture<byte[]> getWithoutTS(ByteArrayWrapper key) {
        Document doc = collection.find(eq("_id", new String(key.getData()))).first();
        if(doc == null)
            return CompletableFuture.completedFuture(null);

        return CompletableFuture.completedFuture(((Binary) doc.get("value")).getData());
    }

    @Override
    public CompletableFuture<GetMessage> get(ByteArrayWrapper key) {
        return null;
    }

    public CompletableFuture<List<byte[]>> scan(Set<ByteArrayWrapper> keyList) {
        List<CompletableFuture<byte[]>> values =  keyList.stream()
                .map(this::getWithoutTS)
                .collect(Collectors.toList());

        return CompletableFuture.allOf(values.toArray(new CompletableFuture[0]))
                .thenApply(future -> values.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));
    }
    @Override
    //xxxResult para o caso de virmos a usar (erros na escrita)
    public CompletableFuture<Void> put(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> timestamp) {
        for(Map.Entry<ByteArrayWrapper, byte[]> kv : writeMap.entrySet()){
            byte[] value = kv.getValue();
            String key = kv.getKey().toString();
            if(value != null){
                Document doc = new Document("_id", key).append("value", value);
                UpdateResult id = collection.replaceOne(eq("_id", key), doc, new ReplaceOptions().upsert(true));
            }
            else {
                DeleteResult id = collection.deleteOne(eq("_id", kv.getKey().toString()));
            }
        }
        return CompletableFuture.completedFuture(null);
    }
}
