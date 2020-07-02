package nosql;


import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.Binary;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import utils.ByteArrayWrapper;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;

public class MongoAsynchKV implements KeyValueDriver {

    private MongoCollection<Document> collection;

    public MongoAsynchKV(String uri, String databaseName, String collectionName) {
        MongoClient mongoClient = MongoClients.create(uri);

        MongoDatabase database = mongoClient.getDatabase(databaseName);
        collection = database.getCollection(collectionName);
    }

    @Override
    public CompletableFuture<byte[]> get(byte[] key) {
        CompletableFuture<byte[]> cf = new CompletableFuture<>();

        collection.find(eq("_id", new String(key))).first()
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
    public CompletableFuture<List<byte[]>> scan(Set<ByteArrayWrapper> keyList) {
        List<CompletableFuture<byte[]>> values =  keyList.stream()
                .map(b -> get(b.getData()))
                .collect(Collectors.toList());

        return CompletableFuture.allOf(values.toArray(new CompletableFuture[0]))
                .thenApply(future -> values.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList())
                );
    }



    @Override
    //xxxResult para o caso de virmos a usar (erros na escrita)
    public CompletableFuture<Void> put(Map<ByteArrayWrapper, byte[]> writeMap) {
        CompletableFuture<Void> cf = new CompletableFuture<>();

        for(Map.Entry<ByteArrayWrapper, byte[]> kv : writeMap.entrySet()){
            byte[] value = kv.getValue();
            String key = kv.getKey().toString();
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
                                cf.complete(null);
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
                                cf.complete(null);
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
        }
        return CompletableFuture.completedFuture(null);
    }




}
