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
import npvs.failuredetection.FailureDetectionService;
import npvs.messaging.FlushMessage;
import npvs.messaging.ReadMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spread.*;
import transaction_manager.utils.ByteArrayWrapper;

import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;

public class NPVSServer {
    private static final Logger LOG = LoggerFactory.getLogger(NPVSServer.class);
    private final ManagedMessagingService mms;
    private final Serializer s;
    private final NPVSImplBS npvs;
    private final int myPort;

    private FailureDetectionService fds;

    private final RaftMessagingService rms;

    private Timestamp<Long> startTs;



    public NPVSServer(int myPort, int spreadPort, String privateName){

        this.myPort = myPort;

        s = new SerializerBuilder()
                .withRegistrationRequired(false)
                .build();
        mms = new NettyMessagingService(
                "server",
                Address.from(myPort),
                new MessagingConfig());
        this.npvs = new NPVSImplBS();

        this.rms = new RaftMessagingService("manager", "127.0.0.1:8081,127.0.0.1:8082");

        int totalServers = 3; // TODO
        // this.fds = new FailureDetectionService(spreadPort, privateName, totalServers);
    }

    public void start() throws UnknownHostException, SpreadException {

        startTs = rms.getTimestamp();

        // TODO voltar a por quando for para usar
        // fds.start();

        mms.start();
        mms.registerHandler("get", (a,b) -> {
            ReadMessage rm = s.decode(b);
            System.out.println(myPort + " get request arrived with key: " + rm.getKey() + " and TS: " + rm.getTs().toPrimitive());
            LOG.info("get request arrived with TS: {}",  rm.getTs().toPrimitive());
            Timestamp<Long> requestTs = rm.getTs();
            if (requestTs.isBefore(startTs))
                return CompletableFuture.completedFuture(s.encode(NPVSReply.FAIL()));

            return npvs.get(rm.getKey(), rm.getTs())
                        .thenApply(s::encode);
        });

        mms.registerHandler("put", (a,b) -> {
            FlushMessage fm = s.decode(b);
            System.out.println(myPort + " put request arrived with TS: " + fm.getTs().toPrimitive());
            LOG.info("put request arrived with TC: {}",  fm.getTs().toPrimitive());
            return npvs.put(fm.getWriteMap(), fm.getTs())
                    .thenApply(s::encode);
        });
    }

    public static void main(String[] args) throws SpreadException, UnknownHostException {

        new NPVSServer(20001, 40000, "0").start();
    }
}
