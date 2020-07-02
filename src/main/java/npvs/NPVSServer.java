package npvs;

import certifier.MonotonicTimestamp;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;
import npvs.binarysearch.NPVSImplBS;
import npvs.messaging.FlushMessage;
import npvs.messaging.ReadMessage;
import utils.ByteArrayWrapper;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NPVSServer {
    private final ManagedMessagingService mms;
    private final ExecutorService e;
    private final Serializer s;
    private final NPVSImplBS npvs;

    public NPVSServer(int port){
        e = Executors.newFixedThreadPool(1);
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
        this.npvs = new NPVSImplBS();
        start();
    }

    private void start(){
        mms.start();

        mms.registerHandler("get", (a,b) -> {
            ReadMessage rm = s.decode(b);
            System.out.println("read request arrived with id: " + rm.getId());
            return npvs.get(rm.getKey(), rm.getTs()).whenComplete((x, y) -> System.out.println(new String(x)))
                                            .thenApply(s::encode);
        });

        mms.registerHandler("put", (a,b) -> {
            System.out.println("flush request arrived");
            FlushMessage fm = s.decode(b);
            npvs.put(fm.getWriteMap(), fm.getMonotonicTimestamp());
            //TODO modificar
            return s.encode(true);
        } ,e);
    }

    public static void main(String[] args) {
        new NPVSServer(20000);
    }
}
