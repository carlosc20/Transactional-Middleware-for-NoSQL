package jraft.rpc;

import transaction_manager.messaging.TransactionContentMessage;

import java.io.Serializable;

public class TransactionCommitRequest implements Serializable {

    private static final long serialVersionUID = -2527003154824547294L;
    private final TransactionContentMessage transactionContentMessage;

    public TransactionCommitRequest(TransactionContentMessage transactionContentMessage){
        this.transactionContentMessage = transactionContentMessage;
    }

    public TransactionContentMessage getTransactionContentMessage() {
        return transactionContentMessage;
    }
}
