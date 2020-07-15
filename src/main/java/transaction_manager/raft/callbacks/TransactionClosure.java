package transaction_manager.raft.callbacks;

import com.alipay.sofa.jraft.Closure;
import transaction_manager.raft.TransactionManagerOperation;
import transaction_manager.raft.rpc.Response;
import transaction_manager.raft.rpc.ValueResponse;

public abstract class TransactionClosure<T> implements Closure {
    private Response<T> response;
    private TransactionManagerOperation transactionManagerOperation;

    public void setTransactionManagerOperation(TransactionManagerOperation transactionManagerOperation) {
        this.transactionManagerOperation = transactionManagerOperation;
    }

    public TransactionManagerOperation getTransactionManagerOperation() {
        return transactionManagerOperation;
    }

    public Response<T> getValueResponse() {
        return response;
    }

    public void setValueResponse(Response<T> response) {
        this.response = response;
    }

    public void failure(final String errorMsg, final String redirect) {
        final ValueResponse<T> response = new ValueResponse<>();
        response.setSuccess(false);
        response.setErrorMsg(errorMsg);
        response.setRedirect(redirect);
        setValueResponse(response);
    }

    public void success(final T value) {
        final ValueResponse<T> response = new ValueResponse<>();
        response.setValue(value);
        response.setSuccess(true);
        setValueResponse(response);
    }
}
