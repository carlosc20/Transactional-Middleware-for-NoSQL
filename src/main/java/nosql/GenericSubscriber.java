package nosql;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.function.Consumer;

public class GenericSubscriber<T> implements Subscriber<T>{
    private final Consumer<T> callback;

    public GenericSubscriber(Consumer<T> callback){
        this.callback = callback;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        subscription.request(1);  // <--- Data requested and the insertion will now occur
    }

    @Override
    public void onNext(T t) {
        this.callback.accept(t);
    }

    @Override
    public void onError(Throwable throwable) {
        System.out.println("Failed");
    }

    @Override
    public void onComplete() {
        System.out.println("Completed");
    }
}
