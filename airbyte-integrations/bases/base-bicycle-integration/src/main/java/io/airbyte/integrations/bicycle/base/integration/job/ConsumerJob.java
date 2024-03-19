package io.airbyte.integrations.bicycle.base.integration.job;

public interface ConsumerJob<T> {
    public void process(T t);

    public void finish();

}
