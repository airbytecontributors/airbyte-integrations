package io.airbyte.integrations.bicycle.base.integration.job.processor;

import io.airbyte.integrations.bicycle.base.integration.job.config.ConsumerConfig;
import io.airbyte.integrations.bicycle.base.integration.job.config.ProducerConfig;
import io.airbyte.integrations.bicycle.base.integration.job.consumer.BicycleConsumer;
import io.airbyte.integrations.bicycle.base.integration.job.consumer.ConsumerJob;
import io.airbyte.integrations.bicycle.base.integration.job.metrics.EventProcessMetrics;
import io.airbyte.integrations.bicycle.base.integration.job.producer.BicycleProducer;
import io.airbyte.integrations.bicycle.base.integration.job.producer.ProducerJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class EventProcessor<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class.getName());

    private BicycleConsumer<T> consumer;
    private BicycleProducer<T> producer;
    private BlockingQueue<T> blockingQueue;;

    public EventProcessor(ProducerConfig producerConfig, ConsumerConfig consumerConfig,
                          EventProcessMetrics metrics) {
        blockingQueue = new LinkedBlockingQueue<>(producerConfig.getMaxBlockingQueueSize());
        this.producer = new BicycleProducer<>(producerConfig, blockingQueue, metrics);
        this.consumer = new BicycleConsumer<>(consumerConfig, blockingQueue, metrics);
    }

    public BicycleProducer<T> getProducer() {
        return producer;
    }

    public Future submit(ProducerJob<T> job) {
        return producer.submit(job);
    }

    public Future<Boolean> submit(ConsumerJob<T> job) {
        return consumer.submit(job);
    }

    public void stop() {
        producer.stop();
        consumer.stop();
    }

    public void shutdown() {
        if (producer != null) {
            producer.shutdown();
        }
        if (consumer != null) {
            consumer.shutdown();
        }
    }

}
