package npvs.replicatedTesting;

import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.core.idgenerator.AtomicIdGenerator;
import io.atomix.utils.net.Address;

public class Client {
    // run any number of times with program arguments:
    //          client1
    //          client2
    //          ...
    public static void main(String[] args) throws Exception {
        AtomixBuilder builder = Atomix.builder();
        builder.withMemberId(args[0])
                .withMembershipProvider(BootstrapDiscoveryProvider.builder()
                        .withNodes(
                                Node.builder()
                                        .withId("member0")              // contact server
                                        .withAddress(Address.from(7000))
                                        .build(),
                                Node.builder()
                                        .withId("member1")
                                        .withAddress(Address.from(7001))
                                        .build()
                        ).build());

        Atomix atomix = builder.build();
        atomix.start().join();

        // server and client for these primitives
        AtomicIdGenerator id = atomix.getAtomicIdGenerator("myid");

        while(true) {
            System.out.println("next id: "+id.nextId());
            Thread.sleep(1000);
        }
    }
}
