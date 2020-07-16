package nosql;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.function.Consumer;

public class GenericSubscriberForFind<T> implements Subscriber<T> {
    private final Consumer<T> onNextCallback;
    private final Consumer<Void> onCompleteCallback;
    private boolean foundDoc = false;

    public GenericSubscriberForFind(Consumer<T> onNextCallback, Consumer<Void> onCompleteCallback){
        this.onNextCallback = onNextCallback;
        this.onCompleteCallback = onCompleteCallback;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        subscription.request(1);  // <--- Data requested and the insertion will now occur
    }

    @Override
    public void onNext(T t) {
        this.foundDoc = true;
        this.onNextCallback.accept(t);
    }

    @Override
    public void onError(Throwable throwable) {
        throwable.printStackTrace();
    }

    @Override
    public void onComplete() {
        if (!foundDoc)
            onCompleteCallback.accept(null);
    }
}
