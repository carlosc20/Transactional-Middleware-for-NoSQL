package nosql;


import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import nosql.messaging.GetMessage;
import org.bson.Document;
import org.bson.types.Binary;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import utils.ByteArrayWrapper;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


//TODO meter os subscribers genéricos
import static com.mongodb.client.model.Filters.eq;

public class MongoAsynchKV implements KeyValueDriver {

    private final MongoCollection<Document> collection;

    public MongoAsynchKV(String uri, String databaseName, String collectionName) {
        MongoClient mongoClient = MongoClients.create(uri);

        MongoDatabase database = mongoClient.getDatabase(databaseName);
        collection = database.getCollection(collectionName);
    }

    @Override
    public CompletableFuture<byte[]> getWithoutTS(ByteArrayWrapper key) {
        CompletableFuture<byte[]> cf = new CompletableFuture<>();

        collection.find(eq("_id", new String(key.getData()))).first()
                .subscribe(new Subscriber<Document>() {
                    @Override
                    public void onSubscribe(final Subscription s) {
                        s.request(1);  // <--- Data requested and the insertion will now occur
                    }

                    @Override
                    public void onNext(final Document result) {
                        if(result == null) {
                            cf.complete(null);
                        }
                        else {
                            cf.complete(((Binary) result.get("value")).getData());
                        }
                    }

                    @Override
                    public void onError(final Throwable t) {
                        System.out.println("Failed");
                    }

                    @Override
                    public void onComplete() {
                        System.out.println("Completed");
                    }
                });
        return cf;
    }

    @Override
    public CompletableFuture<GetMessage> get(ByteArrayWrapper key) {
        return getWithoutTS(key).thenCompose(value -> {
            CompletableFuture<Long> cf = new CompletableFuture<>();
            collection.find().first().subscribe(new Subscriber<Document>() {
                @Override
                public void onSubscribe(final Subscription s) {
                    s.request(1);  // <--- Data requested and the insertion will now occur
                }

                @Override
                public void onNext(final Document result) {
                    if(result == null) {
                        cf.complete(null);
                    }
                    else {
                        cf.complete((Long) result.get("value"));
                    }
                }
                @Override
                public void onError(final Throwable t) {
                    System.out.println("Failed");
                }

                @Override
                public void onComplete() {
                    System.out.println("Completed");
                }
            });
            return cf.thenApply(ts -> new GetMessage(value, new MonotonicTimestamp(ts)));
        });
    }

    @Override
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
    //TODO antes de escrever os tuplos escrever o timestamp
    //xxxResult para o caso de virmos a usar (erros na escrita)
    public CompletableFuture<Void> put(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> timestamp) {
        CompletableFuture<?>[] futures = new CompletableFuture<?>[writeMap.size()];
        for(int i = 0; i < writeMap.size(); i++)
            futures[i] = new CompletableFuture<>();

        int location = 0;
        for(Map.Entry<ByteArrayWrapper, byte[]> kv : writeMap.entrySet()){
            byte[] value = kv.getValue();
            String key = kv.getKey().toString();
            final int local_l = location;
            if(value != null){
                Document doc = new Document("_id", key).append("value", value);
                collection.replaceOne(eq("_id", key), doc, new ReplaceOptions().upsert(true))
                    .subscribe(new Subscriber<UpdateResult>() {
                        @Override
                        public void onSubscribe(final Subscription s) {
                            s.request(1);  // <--- Data requested and the insertion will now occur
                        }

                        @Override
                        public void onNext(final UpdateResult result) {
                            futures[local_l].complete(null);
                        }

                        @Override
                        public void onError(final Throwable t) {
                            System.out.println("Failed");
                        }

                        @Override
                        public void onComplete() {
                            System.out.println("Completed");
                        }
                    });
            }
            else {
                collection.deleteOne(eq("_id", kv.getKey().toString()))
                    .subscribe(new Subscriber<DeleteResult>() {
                        @Override
                        public void onSubscribe(final Subscription s) {
                            s.request(1);  // <--- Data requested and the insertion will now occur
                        }

                        @Override
                        public void onNext(final DeleteResult result) {
                            futures[local_l].complete(null);
                        }

                        @Override
                        public void onError(final Throwable t) {
                            System.out.println("Failed");
                        }

                        @Override
                        public void onComplete() {
                            System.out.println("Completed");
                        }
                    });
            }
            location++;
        }
        return CompletableFuture.allOf(futures);
    }
}
