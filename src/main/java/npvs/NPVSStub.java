package npvs;

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

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class NPVSStub implements NPVS<Long> {
    private final ManagedMessagingService mms;
    private final ExecutorService e;
    private final Serializer s;
    private final List<Address> servers;

    public NPVSStub(Address port, List<String> servers){
        e = Executors.newFixedThreadPool(1);

        this.servers = new ArrayList<>();
        servers.forEach(v -> this.servers.add(Address.from(v)));

        s = new SerializerBuilder()
                .withRegistrationRequired(false)
                .build();
        mms = new NettyMessagingService(
                "server",
                port,
                new MessagingConfig());
        mms.start();
    }

    @Override
    public CompletableFuture<Void> put(FlushMessage flushMessage) {
        List<Map<ByteArrayWrapper, byte[]>> maps = new ArrayList<>(Collections.nCopies(servers.size(), null));
        for (Map.Entry<ByteArrayWrapper, byte[]> entry : flushMessage.getWriteMap().entrySet()){
            byte[] data = entry.getKey().getData();
            int server = assignServer(data);
            Map<ByteArrayWrapper, byte[]> m = maps.get(server);
            if (m == null) {
                m = new HashMap<>();
                maps.set(server, m);
            }
            m.put(entry.getKey(), entry.getValue());
        }

        CompletableFuture<?>[] futures = new CompletableFuture<?>[servers.size()];
        for (int i = 0; i < servers.size(); i++) {
            //TODO mudar e não mandar só para um. Assim para testes
            Address address = servers.get(0);
            FlushMessage flushMessage1 = new FlushMessage(flushMessage);
            flushMessage1.setWriteMap(maps.get(i));
            futures[i] = mms.sendAndReceive(address, "put", s.encode(flushMessage1), Duration.ofSeconds(10), e)
                    .thenApply(s::decode);
        }
        return CompletableFuture.allOf(futures);
    }

    @Override
    public CompletableFuture<NPVSReply> get(ByteArrayWrapper key, Timestamp<Long> ts) {
        byte[] data = key.getData();
        int server = assignServer(data);
        Address address = servers.get(server);
        ReadMessage rm = new ReadMessage(key, ts);
        return mms.sendAndReceive(address, "get", s.encode(rm), Duration.ofSeconds(10), e)
                .thenApply(s::decode);
    }

    private int assignServer(byte[] key) {
        return Arrays.hashCode(key) % servers.size();
    }
}