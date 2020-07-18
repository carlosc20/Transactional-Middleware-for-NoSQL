package nosql;

import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import com.mongodb.Function;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import nosql.messaging.GetMessage;
import nosql.messaging.ScanMessage;
import org.bson.Document;
import org.bson.types.Binary;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;


//TODO meter os subscribers genéricos
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;

public class MongoAsynchKV implements KeyValueDriver{

    private final MongoCollection<Document> collection;

    public MongoAsynchKV(String uri, String databaseName, String collectionName) {
        MongoClient mongoClient = MongoClients.create(uri);

        MongoDatabase database = mongoClient.getDatabase(databaseName);
        collection = database.getCollection(collectionName);
    }

    @Override
    public CompletableFuture<byte[]> getWithoutTS(ByteArrayWrapper key) {
        CompletableFuture<byte[]> cf = new CompletableFuture<>();

        collection.find(eq("_id", key.toString())).first()
            .subscribe(new GenericSubscriberForFind<>(
                result -> cf.complete(((Binary) result.get("value")).getData()),
                x -> cf.complete(null)));
        return cf;
    }

    @Override
    public CompletableFuture<GetMessage> get(ByteArrayWrapper key) {
        return getWithoutTS(key).thenCompose(value -> {
            CompletableFuture<Long> cf = new CompletableFuture<>();
            collection.find(eq("_id", "timestamp")).first()
                .subscribe(new GenericSubscriberForFind<>(
                    result -> cf.complete((Long) result.get("value")),
                    x -> cf.complete(-1L)));
            return cf.thenApply(ts -> new GetMessage(value, new MonotonicTimestamp(ts)));
        });
    }

    @Override
    public CompletableFuture<ScanMessage> scan(Set<ByteArrayWrapper> keyList) {

        List<CompletableFuture<byte[]>> values =  keyList.stream()
            .map(this::getWithoutTS)
            .collect(Collectors.toList());

        return CompletableFuture.allOf(values.toArray(new CompletableFuture[0]))
                .thenApply(future -> values.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList()))
                .thenCompose(vs -> {
                    CompletableFuture<Long> cf = new CompletableFuture<>();
                    collection.find(eq("_id", "timestamp")).first()
                        .subscribe(new GenericSubscriberForFind<>(
                            result -> cf.complete((Long) result.get("value")),
                            x -> cf.complete(-1L)));
                    return cf.thenApply(ts -> new ScanMessage(vs, new MonotonicTimestamp(ts)));
                });
    }


    @Override
    //TODO antes de escrever os tuplos escrever o timestamp
    //xxxResult para o caso de virmos a usar (erros na escrita)
    public CompletableFuture<Void> put(Map<ByteArrayWrapper, byte[]> writeMap) {
        CompletableFuture<?>[] futures = new CompletableFuture<?>[writeMap.size()];
        for(int i = 0; i < writeMap.size(); i++)
            futures[i] = new CompletableFuture<>();

        int location = 0;
        for(Map.Entry<ByteArrayWrapper, byte[]> kv : writeMap.entrySet()){
            byte[] value = kv.getValue();
            String key = kv.getKey().toString();
            if(value != null){
                Document doc = new Document("_id", key).append("value", value);
                replaceOne(key, doc, futures, location);
            }
            else
                deleteOne(key, futures, location);
            location++;
        }
        return CompletableFuture.allOf(futures);
    }

    @Override
    public CompletableFuture<Void> put(Timestamp<Long> timestamp){
        CompletableFuture<Void> cf = new CompletableFuture<>();
        Document doc = new Document("_id", "timestamp").append("value", timestamp.toPrimitive());
        replaceOne("timestamp", doc, cf);
        return cf;
    }

    public void replaceOne(String key, Document doc, CompletableFuture<?>[] futures, final int location){
        collection.replaceOne(eq("_id", key), doc, new ReplaceOptions().upsert(true))
            .subscribe(new GenericSubscriber<>(
                result -> futures[location].complete(null),
                err -> replaceOne(key, doc, futures, location)));
    }


    public void replaceOne(String key, Document doc, CompletableFuture<Void> cf){
        collection.replaceOne(eq("_id", key), doc, new ReplaceOptions().upsert(true))
            .subscribe(new GenericSubscriber<>(
                result -> cf.complete(null),
                err -> replaceOne(key, doc, cf)));
    }

    public void deleteOne(String key, CompletableFuture<?>[] futures, final int location){
        collection.deleteOne(eq("_id", key))
            .subscribe(new GenericSubscriber<>(
                result -> futures[location].complete(null),
                err -> deleteOne(key, futures, location)));
    }

    public void drop() {
        collection.drop();
    }
}
