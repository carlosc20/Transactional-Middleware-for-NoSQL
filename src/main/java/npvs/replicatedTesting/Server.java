package npvs.replicatedTesting;

import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.core.idgenerator.AtomicIdGenerator;
import io.atomix.core.profile.Profile;
import io.atomix.primitive.AsyncPrimitive;
import io.atomix.utils.net.Address;

public class Server {

    // run twice with different working directories and program arguments:
    //          member0 localhost:7000
    //          member1 localhost:7001
    public static void main(String[] args) throws Exception {
        AtomixBuilder builder = Atomix.builder();
        builder.withMemberId(args[0])
                .withAddress(Address.from(args[1]))
                .withProfiles(Profile.consensus("member0","member1"))   // consensus group!
                .withMembershipProvider(BootstrapDiscoveryProvider.builder()
                    .withNodes(
                        Node.builder()
                                .withId("member0")                      // contact server
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
