package npvs;

import certifier.Timestamp;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;
import npvs.binarysearch.NPVSImplBS;
import npvs.treemap.NPVSImplTM;
import utils.ByteArrayWrapper;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NPVSServer {
    private ManagedMessagingService mms;
    private ExecutorService e;
    private Serializer s;
    private NPVSImplBS npvs;

    public NPVSServer(int port){
        e = Executors.newFixedThreadPool(1);
        s = new SerializerBuilder()
                .addType(ReadMessage.class)
                .addType(Timestamp.class)
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

        mms.registerHandler("read", (a,b) -> {
            ReadMessage rm = s.decode(b);
            System.out.println("read request arrived with id: " + rm.id);
            return npvs.read(rm.key, rm.ts).whenComplete((x,y) -> System.out.println(new String(x)))
                                            .thenApply(x -> s.encode(x));
        });

        mms.registerHandler("flush", (a,b) -> {
            System.out.println("flush request arrived");
            FlushMessage fm = s.decode(b);
            npvs.update(fm.writeMap, fm.timestamp);
            //não gostei (0), mas não sei o que por
            return s.encode(0);
        } ,e);
    }

    public static void main(String[] args) {
        new NPVSServer(20000);
    }
}
