package io.airbyte.integrations.source.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.bicycle.base.integration.BicycleConfig;
import io.airbyte.integrations.bicycle.base.integration.MetricAsEventsGenerator;
import io.bicycle.event.publisher.api.BicycleEventPublisher;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ElasticMetricsGenerator extends MetricAsEventsGenerator {

    public ElasticMetricsGenerator(BicycleConfig bicycleConfig, EventSourceInfo eventSourceInfo, JsonNode config,
                                   BicycleEventPublisher bicycleEventPublisher, ElasticsearchSource elasticsearchSource) {
        super(bicycleConfig, eventSourceInfo, config, bicycleEventPublisher, elasticsearchSource);

    }


    @Override
    public void run() {
        try {
            logger.info("Starting the metrics collection");
            Map<String, String> attributes = new HashMap<>();
            attributes.put(UNIQUE_IDENTIFIER, bicycleConfig.getUniqueIdentifier());
            attributes.put(CONNECTOR_ID, eventSourceInfo.getEventSourceId());
            ElasticsearchSource eventConnector = (ElasticsearchSource) this.eventConnector;
            metricsMap.put(getTagEncodedMetricName(TOTAL_EVENTS_PROCESSED_METRIC, attributes),
                    Long.valueOf(eventConnector.getTotalRecordsConsumed()));
            this.publishMetrics(attributes, metricsMap);

        } catch (Exception exception) {
            logger.error("Unable to publish metrics", exception);
        }
    }

}
