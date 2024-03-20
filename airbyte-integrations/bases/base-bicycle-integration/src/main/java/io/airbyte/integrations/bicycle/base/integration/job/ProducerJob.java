package io.airbyte.integrations.bicycle.base.integration.job;

public interface ProducerJob<T> {

    public void process(Producer<T> producer);

    public void finish();

}
