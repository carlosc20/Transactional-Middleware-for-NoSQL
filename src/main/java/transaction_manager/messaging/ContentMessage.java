package transaction_manager.messaging;

import java.io.Serializable;

public class ContentMessage<T extends Serializable> extends Message{

    private T body;

    public ContentMessage(T body){
        super();
        this.body = body;
    }

    public T getBody() {
        return body;
    }

    public String getId() {
        return super.getId();
    }
}
