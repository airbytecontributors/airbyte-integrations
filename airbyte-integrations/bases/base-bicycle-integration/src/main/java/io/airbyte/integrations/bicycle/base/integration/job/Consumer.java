package io.airbyte.integrations.bicycle.base.integration.job;

public interface Consumer<T> {

    public void consume(T t);

}
