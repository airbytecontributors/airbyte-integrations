package io.airbyte.integrations.bicycle.base.integration.job;

import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

public class BicycleEventsProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BicycleRulesProcessor.class.getName());
    private BicycleConsumer<EventProcessorResult> consumer;
    private BicycleProducer<EventProcessorResult> producer;
    private BlockingQueue<EventProcessorResult> blockingQueue;;

    public BicycleEventsProcessor(int poolSize, int queueSize, EventProcessMetrics metrics) {
        blockingQueue = new ArrayBlockingQueue<>(queueSize);
        this.producer = new BicycleProducer<>("bicycle-events-processor-producer", 1, blockingQueue, metrics);
        this.consumer = new BicycleConsumer<>("bicycle-events-processor-consumer", poolSize, blockingQueue, metrics);
    }

    public BicycleProducer<EventProcessorResult> getProducer() {
        return producer;
    }

    public Future submit(ProducerJob<EventProcessorResult> job) {
        return producer.submit(job);
    }

    public Future<Boolean> submit(ConsumerJob<EventProcessorResult> job) {
        return consumer.submit(job);
    }

    public void stop() {
        producer.stop();
        consumer.stop();
    }



}
