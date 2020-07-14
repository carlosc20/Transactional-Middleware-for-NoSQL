package transaction_manager.messaging;

import java.io.Serializable;

public class TransactionCommitRequest extends Message implements Serializable{
    private final TransactionContentMessage transactionContentMessage;

    public TransactionCommitRequest(TransactionContentMessage transactionContentMessage){
        this.transactionContentMessage = transactionContentMessage;
    }

    public TransactionContentMessage getTransactionContentMessage() {
        return transactionContentMessage;
    }
}
