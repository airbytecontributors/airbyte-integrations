package io.airbyte.integrations.bicycle.base.integration.job;

import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class BicycleRulesProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BicycleRulesProcessor.class.getName());
    private BicycleConsumer<RawEvent> consumer;
    private BicycleProducer<RawEvent> producer;
    private BlockingQueue<RawEvent> blockingQueue;

    public BicycleRulesProcessor(int poolSize, int queueSize, EventProcessMetrics metrics) {
        this.blockingQueue = new ArrayBlockingQueue<>(queueSize);
        this.producer = new BicycleProducer<>("bicycle-rules-processor-producer", 1, blockingQueue, metrics);
        this.consumer = new BicycleConsumer<>("bicycle-rules-processor-consumer", poolSize, blockingQueue, metrics);
    }

    public Future<Boolean> submit(ProducerJob<RawEvent> job) {
        return producer.submit(job);
    }

    public Future<Boolean> submit(ConsumerJob<RawEvent> job) {
        return consumer.submit(job);
    }

    public void stop() {
        producer.stop();
        consumer.stop();
    }
}
