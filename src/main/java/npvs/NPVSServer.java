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
import npvs.binarysearch.NPVSImplBSConcurrent;
import npvs.messaging.FlushMessage;
import npvs.messaging.ReadMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spread.*;

import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;

public class NPVSServer {
    private static final Logger LOG = LoggerFactory.getLogger(NPVSServer.class);
    private final ManagedMessagingService mms;
    private final Serializer s;
    private final NPVS<Long> npvs;
    private final int myPort;

    private RaftMessagingService rms;

    //TODO arranjar
    private Timestamp<Long> startTs = new MonotonicTimestamp(-1);

    public NPVSServer(int myPort){
        this.myPort = myPort;
        s = new SerializerBuilder()
                .withRegistrationRequired(false)
                .build();
        mms = new NettyMessagingService(
                "server",
                Address.from(myPort),
                new MessagingConfig());
        this.npvs =  new NPVSImplBS(); //new NPVSImplBSConcurrent();

       // this.rms = new RaftMessagingService("manager", "127.0.0.1:8081,127.0.0.1:8082");
    }

    public void start() throws UnknownHostException, SpreadException {

        //startTs = rms.getTimestamp();

        // TODO voltar a por quando for para usar
        // fds.start();

        mms.start();
        mms.registerHandler("get", (a,b) -> {
            ReadMessage rm = s.decode(b);
            if (rm == null) {
                LOG.info("Received null on get, probably warmup from={}", a.toString());
                return CompletableFuture.completedFuture(s.encode(null));
            }
            // LOG.info("get request arrived with TS: {}",  rm.getTs().toPrimitive());
            Timestamp<Long> requestTs = rm.getTs();
            if (requestTs.isBefore(startTs))
                return CompletableFuture.completedFuture(s.encode(NPVSReply.FAIL()));

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

        mms.registerHandler("eviction", (a,b) -> {
            Timestamp<Long> ts = s.decode(b);
            if (ts == null){
                LOG.info("Received null on eviction, probably warmup from={}", a.toString());
                return CompletableFuture.completedFuture(s.encode(null));
            }
            LOG.info("Eviction request arrived lowWaterMark ={}",ts.toPrimitive());
            //TODO call eviction
            return CompletableFuture.completedFuture(s.encode(null));
        });
    }

    public static void main(String[] args) throws SpreadException, UnknownHostException {
        new NPVSServer(20000).start();
    }
}
