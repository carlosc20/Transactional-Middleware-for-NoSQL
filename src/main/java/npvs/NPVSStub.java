package npvs;

import certifier.Timestamp;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NPVSStub implements NPVS {
    private ManagedMessagingService mms;
    private ExecutorService e;
    private Serializer s;
    private Address npvs;

    public NPVSStub(int port, int npvsPort){
        e = Executors.newFixedThreadPool(1);
        npvs = Address.from(npvsPort);
        s = new SerializerBuilder()
                .addType(ReadMessage.class)
                .addType(Timestamp.class)
                .addType(FlushMessage.class)
                .build();
        mms = new NettyMessagingService(
                "server",
                Address.from(port),
                new MessagingConfig());
        mms.start();
    }


    @Override
    public void update(Map<byte[], byte[]> writeMap, Timestamp ts) {
        FlushMessage fm = new FlushMessage(writeMap, ts);
        mms.sendAndReceive(npvs, "flush", s.encode(fm));
        //.then qualquer coisa se necessário
    }

    @Override
    public CompletableFuture<byte[]> read(byte[] key, Timestamp ts) {
        ReadMessage rm = new ReadMessage(key, ts);
        return mms.sendAndReceive(npvs, "flush", s.encode(rm));
    }
}