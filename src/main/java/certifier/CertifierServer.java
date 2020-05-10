package certifier;

import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.utils.net.Address;

import java.util.concurrent.CompletableFuture;

public class CertifierServer {
/*
    private Atomix atomix;

    public CertifierServer() {
        AtomixBuilder builder = Atomix.builder();
        builder.withMemberId("certifier")
                .withAddress(Address.from(7000))
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

    void start() {
        atomix.getCommunicationService().subscribe("start", message -> {
            return CompletableFuture.completedFuture(message);
        });
        atomix.getCommunicationService().subscribe("commit", message -> {
            return CompletableFuture.completedFuture(message);
        });
        atomix.getCommunicationService().subscribe("update", message -> {
            return CompletableFuture.completedFuture(message);
        });
    }

*/
}
