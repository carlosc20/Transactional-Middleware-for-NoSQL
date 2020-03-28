package npvs;

import certifier.Timestamp;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NPVSServer {
    private ManagedMessagingService mms;
    private ExecutorService e;
    private Serializer s;
    private NPVSImpl npvs;

    public NPVSServer(int port){
        e = Executors.newFixedThreadPool(1);
        s = new SerializerBuilder()
                .addType(ReadMessage.class)
                .addType(Timestamp.class)
                .addType(FlushMessage.class)
                .build();
        mms = new NettyMessagingService(
                "server",
                Address.from(port),
                new MessagingConfig());
        this.npvs = new NPVSImpl();
    }

    private void start(){
        mms.start();
        mms.registerHandler("read", (a,b) -> {
            ReadMessage rm = s.decode(b);
            //acho que não há problema neste join()
            return npvs.read(rm.key, rm.ts).join();

        } ,e);

        mms.registerHandler("flush", (a,b) -> {
            FlushMessage fm = s.decode(b);
            npvs.update(fm.writeMap, fm.timestamp);
            //não gostei (0), mas não sei o que por
            return s.encode(0);
        } ,e);
    }
}
