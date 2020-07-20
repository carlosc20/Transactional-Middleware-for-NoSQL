package transaction_manager.raft.sofa_jraft.rpc;

import java.io.Serializable;

public class ValueResponse<V> implements Serializable, Response<V> {

    private static final long serialVersionUID = -4220017686727146773L;

    private V value;
    private boolean success;

    /**
     * redirect peer id
     */
    private String redirect;

    private String errorMsg;

    public String getErrorMsg() {
        return this.errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public String getRedirect() {
        return this.redirect;
    }

    public void setRedirect(String redirect) {
        this.redirect = redirect;
    }

    public boolean isSuccess() {
        return this.success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public V getValue() {
        return this.value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public ValueResponse(V value, boolean success, String redirect, String errorMsg) {
        super();
        this.value = value;
        this.success = success;
        this.redirect = redirect;
        this.errorMsg = errorMsg;
    }

    public ValueResponse() {
        super();
    }

    @Override
    public String toString() {
        return "ValueResponse [value=" + this.value + ", success=" + this.success + ", redirect=" + this.redirect
                + ", errorMsg=" + this.errorMsg + "]";
    }

}