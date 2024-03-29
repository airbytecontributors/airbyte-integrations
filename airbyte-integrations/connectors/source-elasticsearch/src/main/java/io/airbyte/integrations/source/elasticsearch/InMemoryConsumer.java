package io.airbyte.integrations.source.elasticsearch;

/**
 * @author sumitmaheshwari
 * Created on 06/04/2023
 */


import static io.airbyte.integrations.bicycle.base.integration.CommonConstants.CONNECTOR_LAG_STRING;
import static io.airbyte.integrations.bicycle.base.integration.MetricAsEventsGenerator.TOTAL_EVENTS_PROCESSED_METRIC;
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
import java.util.concurrent.LinkedBlockingDeque;
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
    private static LinkedBlockingDeque<JsonNodesWithEpoch> queue;

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
        queue = new LinkedBlockingDeque(MAX_QUEUE_SIZE * numberOfConsumers);
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

    public void addEventsToQueue(long startEpoch, long endEpoch, String scrollId, List<JsonNode> jsonNodes) {
        JsonNodesWithEpoch jsonNodesWithEpoch = new JsonNodesWithEpoch(startEpoch, endEpoch, scrollId, jsonNodes);
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
                long endEpoch = jsonNodesWithEpoch.getEndEpoch();
                long startEpoch = jsonNodesWithEpoch.getStartEpoch();
                String scrollId = jsonNodesWithEpoch.getScrollId();
                long size = jsonNodesWithEpoch.getJsonNodes().size();

                try {
                    LOGGER.info("MissingEventsDebugging: Reading records of size {} from queue " +
                                    "for startTime {} and endTime {}", size, startEpoch, endEpoch);
                    if (size == 0) {
                        continue;
                    }
                    AuthInfo authInfo = bicycleConfig.getAuthInfo();
                    List<RawEvent> rawEvents =
                            elasticsearchSource.convertRecordsToRawEvents(jsonNodesWithEpoch.getJsonNodes());
                    EventProcessorResult eventProcessorResult =
                            elasticsearchSource.convertRawEventsToBicycleEvents(authInfo, eventSourceInfo, rawEvents);
                    totalRecordsProcessed += jsonNodesWithEpoch.getJsonNodes().size();
                    LOGGER.info("MissingEventsDebugging: Converted to bicycle events records of size {} " +
                            "for startTime {} and endTime {}", size, startEpoch, endEpoch);
                    try {
                        boolean result =
                                elasticsearchSource.publishEvents(authInfo, eventSourceInfo, eventProcessorResult);
                        if (!result) {
                            LOGGER.warn("MissingEventsDebugging: {} Events not published successfully for stream Id {} " +
                                            "with startTime {} and endTime {}",
                                    jsonNodesWithEpoch.getJsonNodes().size(), eventSourceInfo.getEventSourceId(),
                                    startEpoch, endEpoch);
                            metrics.put(CONNECTOR_LAG_STRING, System.currentTimeMillis() - endEpoch);
                            elasticMetricsGenerator.addMetrics(metrics);
                            queue.offerFirst(jsonNodesWithEpoch);
                            continue;
                        }
                        JsonNode toBeSavedState = elasticsearchSource.getUpdatedState(STATE,
                                jsonNodesWithEpoch.getEndEpoch());
                        LOGGER.info("To be saved state {}", toBeSavedState);
                        elasticsearchSource.setState(authInfo, eventSourceInfo.getEventSourceId(), toBeSavedState);
                        //lag metrics
                        metrics.put(TOTAL_EVENTS_PROCESSED_METRIC, totalRecordsProcessed);
                        metrics.put(CONNECTOR_LAG_STRING, System.currentTimeMillis() - endEpoch);
                        elasticMetricsGenerator.addMetrics(metrics);
                        LOGGER.info("MissingEventsDebugging: {} New events published for startTime {} and endTime {}",
                                jsonNodesWithEpoch.getJsonNodes().size(), startEpoch, endEpoch);
                    } catch (Exception exception) {
                        LOGGER.error("MissingEventsDebugging: Unable to publish bicycle events for stream Id {} " +
                                "with startTime {} and endTime {} because of {}", eventSourceInfo.getEventSourceId(),
                                startEpoch, endEpoch, exception);
                        queue.offerFirst(jsonNodesWithEpoch);
                    }
                } catch (Exception exception) {
                    LOGGER.error("MissingEventsDebugging: Unable to convert raw records to bicycle events for stream Id {} " +
                                    "with startTime {} and endTime {} because of {}", eventSourceInfo.getEventSourceId(),
                            startEpoch, endEpoch, exception);
                   // queue.offerFirst(jsonNodesWithEpoch);
                }

            } catch (Exception e) {
                LOGGER.error("Error while processing and publishing events", e);
            }
        }
    };

    private static class JsonNodesWithEpoch {
        private long startEpoch;
        private long endEpoch;
        private String scrollId;
        private List<JsonNode> jsonNodes;

        public JsonNodesWithEpoch(long startEpoch, long endEpoch, String scrollId, List<JsonNode> jsonNodes) {
            this.startEpoch = startEpoch;
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
        public long getStartEpoch() {
            return startEpoch;
        }
    }
}

