package jraft.rpc;

public interface Response<V> {
    void setSuccess(boolean bool);
    void setErrorMsg(String error);
    void setRedirect(String redirect);
    void setValue(V value);

}
