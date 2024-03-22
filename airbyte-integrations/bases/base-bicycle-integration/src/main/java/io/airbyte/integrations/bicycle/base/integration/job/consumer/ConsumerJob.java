package io.airbyte.integrations.bicycle.base.integration.job.consumer;

public interface ConsumerJob<T> {

    public void process(T t);

    public void finish();

}
