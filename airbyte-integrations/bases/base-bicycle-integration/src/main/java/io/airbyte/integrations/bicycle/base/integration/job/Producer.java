package io.airbyte.integrations.bicycle.base.integration.job;

public interface Producer<T> {

    public void produce(T t);

}
