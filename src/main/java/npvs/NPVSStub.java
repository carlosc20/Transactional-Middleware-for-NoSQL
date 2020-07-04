package npvs;

import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;
import npvs.messaging.FlushMessage;
import npvs.messaging.ReadMessage;
import transaction_manager.utils.ByteArrayWrapper;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class NPVSStub implements NPVS<Long> {
    private final ManagedMessagingService mms;
    private final ExecutorService e;
    private final Serializer s;
    private final Address npvs;

    private int idCount;

    public NPVSStub(int port, int npvsPort){
        e = Executors.newFixedThreadPool(1);
        npvs = Address.from(npvsPort);
        s = new SerializerBuilder()
                .addType(ReadMessage.class)
                .addType(MonotonicTimestamp.class)
                .addType(FlushMessage.class)
                .addType(ByteArrayWrapper.class)
                .build();
        mms = new NettyMessagingService(
                "server",
                Address.from(port),
                new MessagingConfig());
        mms.start();

        idCount = 0;
    }


    @Override
    //TODO cuidado com o return
    public CompletableFuture<Void> put(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> ts) {
        FlushMessage fm = new FlushMessage(writeMap, ts);
        return mms.sendAndReceive(npvs, "put", s.encode(fm), e)
                .thenApply(s::decode);
    }

    @Override
    public CompletableFuture<byte[]> get(ByteArrayWrapper key, Timestamp<Long> ts) {
        this.idCount++;
        ReadMessage rm = new ReadMessage(key, ts, idCount);
        return mms.sendAndReceive(npvs, "get", s.encode(rm), Duration.of(10, ChronoUnit.SECONDS), e)
                .whenComplete((m,t) -> {
                    if(t!=null){
                        t.printStackTrace();
                    }
                    else{
                        System.out.println("completing future message " + new String((byte[])s.decode(m)));
                    }});
    }

    /*
    public static void main(String[] args) throws InterruptedException {
        NPVSStub npvs = new NPVSStub(10000, 20000);

        HashMap<ByteArrayWrapper, byte[]> writeMap = new HashMap<>();
        ByteArrayWrapper baw1 = new ByteArrayWrapper("marco".getBytes());
        ByteArrayWrapper baw2 = new ByteArrayWrapper("carlos".getBytes());

        writeMap.put(baw1, "dantas".getBytes());
        writeMap.put(baw2, "castro".getBytes());
        npvs.update(writeMap, 1);
        writeMap.put(baw1, "dantas2".getBytes());
        npvs.update(writeMap, 2);
        writeMap.put(baw1, "dantas4".getBytes());
        npvs.update(writeMap, 4);
        writeMap.put(baw1, "dantas10".getBytes());
        npvs.update(writeMap, 10);
        Thread.sleep(1000);
        npvs.read(baw1, 3);
        Thread.sleep(1000);
        npvs.read(baw1, 5);
        npvs.read(baw1, 1);
        npvs.read(baw1, 9);
        npvs.read(baw1, 11);
        npvs.read(baw1, 10);
    }
    */
}