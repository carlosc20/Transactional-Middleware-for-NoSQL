package npvs;

import certifier.Timestamp;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.utils.net.Address;

public class NPVSimpl implements NPVS {

    public NPVSimpl() {
        AtomixBuilder builder = Atomix.builder();
        builder.withMemberId("member1")
                .withAddress(Address.from(5000))
                .build();

        builder.withMembershipProvider(BootstrapDiscoveryProvider.builder()
                .withNodes(
                        Node.builder()
                                .withId("member1")
                                .withAddress(Address.from(5000))
                                .build(),
                        Node.builder()
                                .withId("member2")
                                .withAddress(Address.from(5001))
                                .build()
                ).build());

        Atomix atomix = builder.build();
        atomix.start().join();

        // TODO criar primitiva distribuida
    }

    @Override
    public void write(byte[] key, byte[] value, Timestamp ts) {

    }

    @Override
    public byte[] read(byte[] key) {
        return new byte[0];
    }
}
