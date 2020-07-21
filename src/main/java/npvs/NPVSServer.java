package npvs;

import certifier.MonotonicTimestamp;
import certifier.Timestamp;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.SerializerBuilder;
import npvs.binarysearch.NPVSImplBS;
import npvs.messaging.FlushMessage;
import npvs.messaging.ReadMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;

public class NPVSServer {
    private static final Logger LOG = LoggerFactory.getLogger(NPVSServer.class);
    private final ManagedMessagingService mms;
    private final Serializer s;
    private final AbstractNPVS npvs;
    private NPVSRaftClient rms = null;

    public NPVSServer(int myPort, boolean withRaft){

        s = new SerializerBuilder()
                .withRegistrationRequired(false)
                .build();
        mms = new NettyMessagingService(
                "server",
                Address.from(myPort),
                new MessagingConfig());
        this.npvs =  new NPVSImplBS(); //new NPVSImplBSConcurrent();
        if (withRaft)
            this.rms = new NPVSRaftClient("manager", "127.0.0.1:8081,127.0.0.1:8082");
    }

    public void start(){
        if (rms != null)
            this.npvs.setRebootTs(rms.getTimestamp());
        mms.start();
        mms.registerHandler("get", (a,b) -> {
            ReadMessage rm = s.decode(b);
            if (rm == null) {
                LOG.info("Received null on get, probably warmup from={}", a.toString());
                return CompletableFuture.completedFuture(s.encode(null));
            }
            // LOG.info("get request arrived with TS: {}",  rm.getTs().toPrimitive());
            return npvs.get(rm.getKey(), rm.getTs())
                        .thenApply(s::encode);
        });

        mms.registerHandler("put", (a,b) -> {
            FlushMessage fm = s.decode(b);
            if (fm == null){
                LOG.info("Received null on put, probably warmup from={}", a.toString());
                return CompletableFuture.completedFuture(s.encode(null));
            }
            // LOG.info("put request arrived with TC: {} with id: {}", fm.getCurrentTimestamp().toPrimitive(), fm.getTransactionStartTimestamp().toPrimitive());
            return npvs.put(fm).thenApply(s::encode);
        });

        mms.registerHandler("evict", (a,b) -> {
            Timestamp<Long> ts = s.decode(b);
            if (ts != null){
                LOG.info("Eviction request arrived lowWaterMark ={}",ts.toPrimitive());
                npvs.evict(ts);
            }
            return CompletableFuture.completedFuture(s.encode(null));
        });
        System.out.println("NPVS server ready");
    }

    public static void main(String[] args) throws UnknownHostException {
        new NPVSServer(20001, false).start();
    }
}
