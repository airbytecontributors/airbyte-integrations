package io.airbyte.integrations.source.elasticsearch;

/**
 * @author sumitmaheshwari
 * Created on 06/04/2023
 */

import static io.airbyte.integrations.bicycle.base.integration.MetricAsEventsGenerator.TOTAL_EVENTS_PROCESSED_METRIC;
import static io.airbyte.integrations.source.elasticsearch.ElasticsearchSource.ELASTIC_LAG;
import static io.airbyte.integrations.source.elasticsearch.ElasticsearchSource.STATE;
import com.fasterxml.jackson.databind.JsonNode;
import com.inception.server.auth.model.AuthInfo;
import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryConsumer.class);
    // Define the maximum size of the queue
    private static final int MAX_QUEUE_SIZE = 10;

    // Define the blocking queue
    private static BlockingQueue<JsonNodesWithEpoch> queue;

    // Define the thread pool executor with 2 threads
    ScheduledThreadPoolExecutor executor ;
    private ElasticsearchSource elasticsearchSource;
    private BicycleConfig bicycleConfig;
    private EventSourceInfo eventSourceInfo;
    private ElasticMetricsGenerator elasticMetricsGenerator;
    private long totalRecordsProcessed = 0;
    private List<ScheduledFuture> scheduledFutures = new ArrayList<>();
    private Map<String, Long> metrics = new HashMap<>();

    public InMemoryConsumer(ElasticsearchSource elasticsearchSource, BicycleConfig bicycleConfig,
                            EventSourceInfo eventSourceInfo, ElasticMetricsGenerator elasticMetricsGenerator,
                            int numberOfConsumers) {
        this.elasticsearchSource = elasticsearchSource;
        this.bicycleConfig = bicycleConfig;
        this.eventSourceInfo = eventSourceInfo;
        this.elasticMetricsGenerator = elasticMetricsGenerator;
        executor = new ScheduledThreadPoolExecutor(numberOfConsumers);
        for (int i = 0; i < numberOfConsumers; i++) {
            scheduledFutures.add(executor.schedule(consumer, 1, TimeUnit.SECONDS));
        }
        queue = new ArrayBlockingQueue<>(MAX_QUEUE_SIZE * numberOfConsumers);
    }

    public void rescheduleIfStopped() {
        List<ScheduledFuture> newList = new ArrayList<>();
        for (ScheduledFuture scheduledFuture: scheduledFutures) {
            if (scheduledFuture.isDone() || scheduledFuture.isCancelled()) {
                LOGGER.info("Rescheduling again");
                scheduledFuture = executor.schedule(consumer, 0, TimeUnit.SECONDS);
            }
            newList.add(scheduledFuture);
        }
        scheduledFutures = newList;
    }

    public void addEventsToQueue(long endEpoch, String scrollId, List<JsonNode> jsonNodes) {
        JsonNodesWithEpoch jsonNodesWithEpoch = new JsonNodesWithEpoch(endEpoch, scrollId, jsonNodes);
        try {
            queue.offer(jsonNodesWithEpoch, 10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("Unable to add events to queue", e);
        }
    }

    // Define the consumer task
    private Runnable consumer = () -> {

        while (true) {
            try {

                JsonNodesWithEpoch jsonNodesWithEpoch = queue.take();

                try {
                    long endEpoch = jsonNodesWithEpoch.getEndEpoch();
                    String scrollId = jsonNodesWithEpoch.getScrollId();
                    long size = jsonNodesWithEpoch.getJsonNodes().size();
                    LOGGER.info("Reading records of size {} from queue ", size);
                    AuthInfo authInfo = bicycleConfig.getAuthInfo();
                    List<RawEvent> rawEvents =
                            elasticsearchSource.convertRecordsToRawEvents(jsonNodesWithEpoch.getJsonNodes());
                    EventProcessorResult eventProcessorResult =
                            elasticsearchSource.convertRawEventsToBicycleEvents(authInfo, eventSourceInfo, rawEvents);
                    totalRecordsProcessed += jsonNodesWithEpoch.getJsonNodes().size();
                    try {
                        boolean result =
                                elasticsearchSource.publishEvents(authInfo, eventSourceInfo, eventProcessorResult);
                        if (!result) {
                            LOGGER.warn("Events not published successfully for stream Id {}",
                                    eventSourceInfo.getEventSourceId());
                            metrics.put(ELASTIC_LAG, System.currentTimeMillis() - endEpoch);
                            elasticMetricsGenerator.addMetrics(metrics);
                            continue;
                        }
                        JsonNode toBeSavedState = elasticsearchSource.getUpdatedState(STATE,
                                jsonNodesWithEpoch.getEndEpoch());
                        LOGGER.info("To be saved state {}", toBeSavedState);
                        elasticsearchSource.setState(authInfo, eventSourceInfo.getEventSourceId(), toBeSavedState);
                        //lag metrics
                        metrics.put(TOTAL_EVENTS_PROCESSED_METRIC, totalRecordsProcessed);
                        metrics.put(ELASTIC_LAG, System.currentTimeMillis() - endEpoch);
                        elasticMetricsGenerator.addMetrics(metrics);
                        LOGGER.info("New events published for endpoch {} with scrollId {} and size :{}", endEpoch,
                                scrollId, size);
                    } catch (Exception exception) {
                        LOGGER.error("Unable to publish bicycle events", exception);
                    }
                } catch (Exception exception) {
                    LOGGER.error("Unable to convert raw records to bicycle events", exception);
                }

            } catch (Exception e) {
                LOGGER.error("Error while processing and publishing events", e);
            }
        }
    };

    private static class JsonNodesWithEpoch {
        private long endEpoch;
        private String scrollId;
        private List<JsonNode> jsonNodes;

        public JsonNodesWithEpoch(long endEpoch, String scrollId, List<JsonNode> jsonNodes) {
            this.endEpoch = endEpoch;
            this.scrollId = scrollId;
            this.jsonNodes = jsonNodes;
        }

        public long getEndEpoch() {
            return endEpoch;
        }

        public String getScrollId() {
            return scrollId;
        }

        public List<JsonNode> getJsonNodes() {
            return jsonNodes;
        }
    }
}

