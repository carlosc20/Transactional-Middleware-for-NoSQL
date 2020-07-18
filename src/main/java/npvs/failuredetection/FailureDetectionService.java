package npvs.failuredetection;

import spread.*;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class FailureDetectionService {
    private final int spreadPort;
    private final SpreadGroup spreadGroup;
    private final SpreadConnection spreadConnection;
    private final String privateName;
    private final int totalServers;
    private SpreadGroup[] members;
    private final Map<SpreadGroup, Boolean> crashed;


    enum State {
        WAITING_OK,
        WAITING_ALL,
        OK
    }

    private State state;


    public FailureDetectionService(int spreadPort, String privateName, int totalServers) {

        this.privateName = privateName;
        this.spreadPort = spreadPort;
        this.spreadGroup = new SpreadGroup();
        this.spreadConnection = new SpreadConnection();
        this.totalServers = totalServers;
        this.crashed = new HashMap<>();
        this.state = State.WAITING_OK;
    }

    public void start() throws UnknownHostException, SpreadException {
        this.spreadConnection.connect(InetAddress.getByName("localhost"), spreadPort, this.privateName,
                false, true);
        this.spreadGroup.join(this.spreadConnection, "group");
        this.spreadConnection.add(messageListener());

        // TODO espera até saber se está ok ou nao?
    }

    public AdvancedMessageListener messageListener() {
        return new AdvancedMessageListener() {
            @Override
            public void regularMessageReceived(SpreadMessage spreadMessage) {
                try {
                    if(state != State.OK) {
                        boolean isOk = (Boolean) spreadMessage.getObject();
                        if (isOk) { // is waiting for all members to ready
                            if (members.length == totalServers)
                                state = State.OK;
                            else
                                state = State.WAITING_ALL;
                        }
                        else { // crashed
                            // TODO recuperar, callback do npvs? npvs depois poe OK?
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            @Override
            public void membershipMessageReceived(SpreadMessage spreadMessage) {
                try {
                    MembershipInfo info = spreadMessage.getMembershipInfo();

                    if(info.isRegularMembership()) {
                        members = info.getMembers();
                    } else return;

                    if(info.isCausedByJoin()) {
                        handleJoin(info);
                    }
                    else if(info.isCausedByNetwork()) {
                        handleNetworkPartition(info);
                    }
                    else if(info.isCausedByDisconnect()) {
                        handleDisconnect(info);
                    }
                    else if(info.isCausedByLeave()) {
                        handleLeave(info);
                    }
                } catch(Exception e){
                    e.printStackTrace();
                }
            }
        };
    }

    private void handleNetworkPartition(MembershipInfo info) {
        SpreadGroup[] stayed = info.getStayed(); // usar getMembers?
        System.out.println(privateName + ": MembershipMessage received -> network partition");
        System.out.println(privateName + ": Partition members: " + Arrays.toString(stayed));
        // TODO
    }

    private void handleDisconnect(MembershipInfo info) {
        SpreadGroup member = info.getDisconnected();
        System.out.println(privateName + ": MembershipMessage received -> disconnect");
        handleDL(member);
    }

    private void handleLeave(MembershipInfo info) {
        SpreadGroup member = info.getLeft();
        System.out.println(privateName + ": MembershipMessage received -> left");
        handleDL(member);
    }

    private void handleDL(SpreadGroup member) {
        System.out.println(privateName + ": Member: " + member);
        crashed.put(member, true);
    }


    private void handleJoin(MembershipInfo info) throws Exception {
        SpreadGroup newMember = info.getJoined();
        if(newMember.equals(spreadConnection.getPrivateGroup())) { // self join
            handleSelfJoin();
            return;
        }
        System.out.println(privateName + ": MembershipMessage received -> join");
        System.out.println(privateName + ": Member: " + newMember);

        if (state == State.WAITING_OK) return;

        // checks if joined has crashed
        Boolean hasCrashed = crashed.get(newMember);
        if (hasCrashed == null || !hasCrashed) {
            if (state == State.WAITING_ALL) {
                if (members.length == totalServers) {
                    state = State.OK; // all servers are ready
                }
                floodMessage(Boolean.TRUE, newMember, "reliable"); // TODO ao receber ve se tao todos ou n
            }
            crashed.put(newMember, false); // necessário?
        }
        else {
            if (state == State.WAITING_ALL)
                System.out.println("enviar msg a dizer waiting for all"); // crash is ok if still waiting for all

            floodMessage(Boolean.FALSE, newMember, "reliable"); // TODO enviar msg a dizer que falhou
        }

    }
    private void handleSelfJoin() {
        if (members.length == 1) { // im first member
            state = State.WAITING_ALL;
        } else {
            state = State.WAITING_OK;
        }
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
        System.out.println("Flooding to group ("+ this.spreadGroup+ "): ");
    }
}
