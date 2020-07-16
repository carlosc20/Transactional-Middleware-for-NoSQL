package nosql;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.function.Consumer;

public class GenericSubscriber<T> implements Subscriber<T>{
    private final Consumer<T> onNextCallback;

    public GenericSubscriber(Consumer<T> onNextCallback){
        this.onNextCallback = onNextCallback;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        subscription.request(1);  // <--- Data requested and the insertion will now occur
    }

    @Override
    public void onNext(T t) {
        this.onNextCallback.accept(t);
    }

    @Override
    public void onError(Throwable throwable) {
        throwable.printStackTrace();
    }

    @Override
    public void onComplete() {}
}
