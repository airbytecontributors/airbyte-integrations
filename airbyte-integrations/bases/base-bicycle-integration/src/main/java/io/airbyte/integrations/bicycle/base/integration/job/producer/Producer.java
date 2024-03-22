package io.airbyte.integrations.bicycle.base.integration.job.producer;

public interface Producer<T> {

    public void produce(T t);

}
