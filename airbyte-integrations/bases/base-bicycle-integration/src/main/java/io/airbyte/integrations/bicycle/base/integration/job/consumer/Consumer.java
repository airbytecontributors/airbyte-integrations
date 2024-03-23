package io.airbyte.integrations.bicycle.base.integration.job.consumer;

public interface Consumer<T> {

    public void consume(T t);

}
