package jraft.rpc;

import transaction_manager.messaging.Message;
import transaction_manager.messaging.TransactionContentMessage;

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
