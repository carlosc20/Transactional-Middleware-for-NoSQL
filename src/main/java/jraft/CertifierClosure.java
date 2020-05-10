package jraft;

import com.alipay.sofa.jraft.Closure;
import jraft.rpc.Response;
import jraft.rpc.ValueResponse;

public abstract class CertifierClosure<V> implements Closure {
    private Response<V> response;
    private CertifierOperation certifierOperation;

    public void setCertifierOperation(CertifierOperation certifierOperation) {
        this.certifierOperation = certifierOperation;
    }

    public CertifierOperation getCertifierOperation() {
        return certifierOperation;
    }

    public Response<V> getValueResponse() {
        return response;
    }

    public void setValueResponse(Response<V> response) {
        this.response = response;
    }

    protected void failure(final String errorMsg, final String redirect) {
        final ValueResponse<V> response = new ValueResponse<>();
        response.setSuccess(false);
        response.setErrorMsg(errorMsg);
        response.setRedirect(redirect);
        setValueResponse(response);
    }

    protected void success(final V value) {
        final ValueResponse<V> response = new ValueResponse<>();
        response.setValue(value);
        response.setSuccess(true);
        setValueResponse(response);
    }
}
