package transaction_manager.raft.sofa_jraft.rpc;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import transaction_manager.raft.sofa_jraft.callbacks.TransactionClosure;

import java.util.function.BiConsumer;


public class RequestProcessor<T1, T2> implements RpcProcessor<T1> {
    private final Class<T1> requestClass;
    private final BiConsumer<T1, TransactionClosure<T2>> handler;

    public RequestProcessor(Class<T1> requestClass, BiConsumer<T1, TransactionClosure<T2>> handler){
        super();
        this.requestClass = requestClass;
        this.handler = handler;
    }

    @Override
    public void handleRequest(RpcContext rpcContext, T1 t) {
        final TransactionClosure<T2> closure = new TransactionClosure<T2>() {
            public void run(Status status) {
                rpcContext.sendResponse(getValueResponse());
            }
        };
        handler.accept(t, closure);
    }

    @Override
    public String interest() {
        return requestClass.getName();
    }
}
