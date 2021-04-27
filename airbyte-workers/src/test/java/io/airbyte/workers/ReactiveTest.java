package io.airbyte.workers;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Observable;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import io.reactivex.Flowable;
public class ReactiveTest {
  @Test
  void test() {
    Observable<Integer> integerObservable = Observable.just(1, 2, 3);
    Flowable<Integer> integerFlowable = integerObservable.toFlowable(BackpressureStrategy.BUFFER);

    Stream.of(1,2,3).flatMap().ta
  }
}
