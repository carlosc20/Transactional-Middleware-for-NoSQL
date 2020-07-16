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
import spread.*;
import transaction_manager.utils.ByteArrayWrapper;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class NPVSServer {
    private static final Logger LOG = LoggerFactory.getLogger(NPVSServer.class);
    private final ManagedMessagingService mms;
    private final Serializer s;
    private final NPVSImplBS npvs;

    private final int myPort;
    private final int spreadPort;
    private final SpreadGroup spreadGroup;
    private final SpreadConnection spreadConnection;
    private final String privateName;

    public NPVSServer(int myPort, int spreadPort, String privateName){

        this.myPort = myPort;
        this.privateName = privateName;

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

        this.spreadPort = spreadPort;
        this.spreadGroup = new SpreadGroup();
        this.spreadConnection = new SpreadConnection();
    }

    public void start() throws UnknownHostException, SpreadException {

        /*
        this.spreadConnection.connect(InetAddress.getByName("localhost"), spreadPort, this.privateName,
                false, true);
        this.spreadGroup.join(this.spreadConnection, "npvs");
        this.spreadConnection.add(messageListener());
        */

        mms.start();
        mms.registerHandler("get", (a,b) -> {
            ReadMessage rm = s.decode(b);
            System.out.println(myPort + " get request arrived with key: " + rm.getKey() + " and TS: " + rm.getTs().toPrimitive());
            LOG.info("get request arrived with TS: {}",  rm.getTs().toPrimitive());
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


    public AdvancedMessageListener messageListener() {
        return new AdvancedMessageListener() {
            @Override
            public void regularMessageReceived(SpreadMessage spreadMessage) {
                try {
                    // Message received = (Message) spreadMessage.getObject();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            @Override
            public void membershipMessageReceived(SpreadMessage spreadMessage) {
                try {
                    MembershipInfo info = spreadMessage.getMembershipInfo();

                    if(info.isRegularMembership()) {
                        //members = info.getMembers();
                    } else return;

                    if(info.isCausedByJoin()) {
                        //handleJoin(info);
                    }
                    else if(info.isCausedByNetwork()) {
                        //handleNetworkPartition(info);
                    }
                    else if(info.isCausedByDisconnect()) {
                        //handleDisconnect(info);
                    }
                    else if(info.isCausedByLeave()) {
                        //handleLeave(info);
                    }
                } catch(Exception e){
                    e.printStackTrace();
                }
            }
        };
    }

    public void floodMessage(Serializable message, SpreadGroup sg, String type) throws Exception{
        SpreadMessage m = new SpreadMessage();
        m.addGroup(sg);
        m.setObject(message);
        switch (type) {
            case "reliable":
                m.setReliable();
                break;
            case "fifo":
                m.setFifo();
                break;
            case "causal":
                m.setCausal();
                break;
            case "agreed":
                m.setAgreed();
                break;
            case "safe":
                m.setSafe();
                break;
            default:
                m.setUnreliable();
        }
        spreadConnection.multicast(m);
        System.out.println("Safe flooding to group ("+ this.spreadGroup+ "): ");
    }

    public static void main(String[] args) throws SpreadException, UnknownHostException {

        new NPVSServer(20000, 40000, "0").start();
    }
}
