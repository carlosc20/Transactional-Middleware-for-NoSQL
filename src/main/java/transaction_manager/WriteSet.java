package transaction_manager;

public interface WriteSet {
    boolean contains(byte[] key);
    void insert(byte[] key);
    byte[] read(byte[] key);
    void addWriteOp(byte[] key, byte[] value);
    void addDeleteOp(byte[] key);
    /*
    When the flush operation is called, the transaction may holdseveral operations in its write-set that can invalidate themselves(e.g.  write(x)  and  delete(x)).
     To  avoid  situations  like  such, the  commit  operation  submits  the  transaction’s  write-set  to  aconciliation procedure that removes such issues.
     */
    void optimize();
}
