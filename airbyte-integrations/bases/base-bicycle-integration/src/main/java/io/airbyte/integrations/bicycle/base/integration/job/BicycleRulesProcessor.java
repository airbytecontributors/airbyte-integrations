package io.airbyte.integrations.bicycle.base.integration.job;

import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class BicycleRulesProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(BicycleRulesProcessor.class.getName());
    private BicycleConsumer<RawEvent, Boolean> consumer;
    private BicycleProducer<RawEvent, Boolean> producer;
    private BlockingQueue<RawEvent> blockingQueue = new ArrayBlockingQueue<>(100);

    public BicycleRulesProcessor(int poolSize, EventProcessMetrics metrics, ConsumerJob<RawEvent> job) {
        this.producer = new BicycleProducer<>("bicycle-rules-processor-producer", 1, blockingQueue, metrics);
        this.consumer = new BicycleConsumer<>("bicycle-rules-processor-consumer", poolSize, blockingQueue, job, metrics);
    }

    public Future<Boolean> submit(ProducerJob<RawEvent> job) {
        return producer.submit(job);
    }

    public void stop() {
        producer.stop(false);
        consumer.stop(false);
    }
}
