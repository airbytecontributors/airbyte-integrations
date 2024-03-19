package io.airbyte.integrations.bicycle.base.integration.job;

import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

public class BicycleEventsProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BicycleRulesProcessor.class.getName());
    private BicycleConsumer<EventProcessorResult, Boolean> consumer;
    private BicycleProducer<EventProcessorResult, Boolean> producer;
    private BlockingQueue<EventProcessorResult> blockingQueue = new ArrayBlockingQueue<>(100);

    public BicycleEventsProcessor(int poolSize, EventProcessMetrics metrics, ConsumerJob<EventProcessorResult> job) {
        this.producer = new BicycleProducer<>("bicycle-events-processor-producer", 1, blockingQueue, metrics);
        this.consumer = new BicycleConsumer<>("bicycle-events-processor-consumer", poolSize, blockingQueue, job, metrics);
    }

    public Future submit(ProducerJob<EventProcessorResult> job) {
        return producer.submit(job);
    }

    public void stop() {
        producer.stop(false);
        consumer.stop(false);
    }



}
