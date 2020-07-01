package jraft;

import certifier.Timestamp;
import transaction_manager.BitWriteSet;

public interface CertifierService {
    void getTimestamp(final boolean readOnlySafe, final CertifierClosure<Timestamp> closure);
    void commit(final BitWriteSet bws, final Timestamp timestamp, final CertifierClosure<Timestamp> closure);
}