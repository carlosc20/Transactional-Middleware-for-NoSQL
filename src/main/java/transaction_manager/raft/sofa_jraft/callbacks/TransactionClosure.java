package transaction_manager.raft.sofa_jraft.callbacks;

import com.alipay.sofa.jraft.Closure;
import transaction_manager.raft.sofa_jraft.StateMachineOperation;
import transaction_manager.raft.sofa_jraft.rpc.Response;
import transaction_manager.raft.sofa_jraft.rpc.ValueResponse;

public abstract class TransactionClosure<T> implements Closure {
    private Response<T> response;
    private StateMachineOperation stateMachineOperation;

    public void setStateMachineOperation(StateMachineOperation stateMachineOperation) {
        this.stateMachineOperation = stateMachineOperation;
    }

    public StateMachineOperation getStateMachineOperation() {
        return stateMachineOperation;
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
