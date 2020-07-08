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

    public NPVSStub(int port, int npvsPort){
        e = Executors.newFixedThreadPool(1);
        npvs = Address.from(npvsPort);
        s = new SerializerBuilder()
                .addType(FlushMessage.class)
                .addType(ReadMessage.class)
                .addType(ByteArrayWrapper.class)
                .addType(MonotonicTimestamp.class)
                .build();
        mms = new NettyMessagingService(
                "server",
                Address.from(port),
                new MessagingConfig());
        mms.start();
    }

    @Override
    //TODO cuidado com o return
    public CompletableFuture<Void> put(Map<ByteArrayWrapper, byte[]> writeMap, Timestamp<Long> ts) {
        FlushMessage fm = new FlushMessage(writeMap, ts);
        return mms.sendAndReceive(npvs, "put", s.encode(fm), Duration.ofSeconds(10), e)
                .thenApply(s::decode);
    }

    @Override
    public CompletableFuture<byte[]> get(ByteArrayWrapper key, Timestamp<Long> ts) {
        ReadMessage rm = new ReadMessage(key, ts);
        return mms.sendAndReceive(npvs, "get", s.encode(rm), Duration.ofSeconds(10), e)
                .thenApply(s::decode);
    }
}