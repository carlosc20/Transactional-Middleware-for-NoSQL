package certifier;

import io.atomix.cluster.MemberId;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.utils.net.Address;
import transaction_manager.BitWriteSet;


public class CertifierStub {
/*
    private Atomix atomix;

    public CertifierStub(String uri) {
        AtomixBuilder builder = Atomix.builder();
        builder.withMemberId("member1")
                .withAddress(Address.from(7001))
                .build();

        builder.withMembershipProvider(BootstrapDiscoveryProvider.builder()
                .withNodes(
                        Node.builder()
                                .withId("certifier")
                                .withAddress(Address.from(7000))
                                .build(),
                        Node.builder()
                                .withId("member1")
                                .withAddress(Address.from(7001))
                                .build()
                ).build());

        atomix = builder.build();
        atomix.start().join();
    }

    @Override
    public Timestamp start() {
        try {
            return new Timestamp(atomix.getCommunicationService().send("start", null, MemberId.from("certifier")).thenApply(response -> {
                return (int) response;
            }).get());
        } catch (Exception e) {
            e.printStackTrace();
            return new Timestamp(-1);
        }
    }

    @Override
    public Timestamp commit(BitWriteSet ws) {
        try {
            return new Timestamp(atomix.getCommunicationService().send("commit", null, MemberId.from("certifier")).thenApply(response -> {
                return (int) response;
            }).get());
        } catch (Exception e) {
            e.printStackTrace();
            return new Timestamp(-1);
        }
    }

    @Override
    public void update() {

    }

 */
}
