package npvs;

import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;
import io.github.classgraph.json.JSONUtils;
import npvs.binarysearch.NPVSImplBS;
import npvs.messaging.FlushMessage;
import npvs.messaging.ReadMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction_manager.utils.ByteArrayWrapper;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NPVSServer {
    private static final Logger LOG = LoggerFactory.getLogger(NPVSServer.class);
    private final ManagedMessagingService mms;
    private final ExecutorService e;
    private final Serializer s;
    private final NPVSImplBS npvs;

    public NPVSServer(int myPort){
        e = Executors.newFixedThreadPool(1);
        s = new SerializerBuilder()
                .addType(FlushMessage.class)
                .addType(ReadMessage.class)
                .addType(ByteArrayWrapper.class)
                .addType(MonotonicTimestamp.class)
                .build();
        mms = new NettyMessagingService(
                "server",
                Address.from(myPort),
                new MessagingConfig());
        this.npvs = new NPVSImplBS();
    }

    private void start(){
        mms.start();
        mms.registerHandler("get", (a,b) -> {
            ReadMessage rm = s.decode(b);
            LOG.info("get request arrived with TS: {}",  rm.getTs().toPrimitive());
            return npvs.get(rm.getKey(), rm.getTs())
                        .thenApply(s::encode);
        });

        mms.registerHandler("put", (a,b) -> {
            FlushMessage fm = s.decode(b);
            LOG.info("put request arrived with TC: {}",  fm.getTs().toPrimitive());
            return npvs.put(fm.getWriteMap(), fm.getTs())
                    .thenApply(s::encode);
        });
    }
    public static void main(String[] args) {
        new NPVSServer(20000).start();
    }
}
