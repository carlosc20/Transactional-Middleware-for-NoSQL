package jraft;

import transaction_manager.BitWriteSet;

public interface CertifierService {
    void getTimestamp(final boolean readOnlySafe, final CertifierClosure<Long> closure);
    void commit(final BitWriteSet bws, final long timestamp, final CertifierClosure<Long> closure);
}