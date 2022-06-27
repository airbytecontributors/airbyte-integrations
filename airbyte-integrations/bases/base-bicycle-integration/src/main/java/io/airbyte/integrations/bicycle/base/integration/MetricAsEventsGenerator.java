package io.airbyte.integrations.bicycle.base.integration;


import ai.apptuit.metrics.client.TagEncodedMetricName;
import ai.apptuit.ml.utils.MetricUtils;
import bicycle.io.events.proto.BicycleEvent;
import bicycle.io.events.proto.BicycleEventList;
import com.fasterxml.jackson.databind.JsonNode;
import com.inception.server.auth.model.AuthInfo;
import io.bicycle.event.publisher.impl.BicycleEventsHelper;
import io.bicycle.server.event.mapping.models.converter.BicycleEventsResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricAsEventsGenerator implements Runnable {

    private static final String TENANT_ID = "tenantId";
    protected static final String UNIQUE_IDENTIFIER = "identifier";
    protected static final String CONNECTOR_ID = "connectorId";
    protected static final String METRIC_NAME_SEPARATOR = "_";
    protected static final String EVENTS_PROCESSED_METRIC = "events_processed";
    protected static final String TOTAL_EVENTS_PROCESSED_METRIC = "total_events_processed";
    protected final Logger logger = LoggerFactory.getLogger(MetricAsEventsGenerator.class.getName());
    protected final Map<String, String> globalTags = new HashMap<>();
    protected BicycleConfig bicycleConfig;
    private AuthInfo bicycleAuthInfo;
    private JsonNode config;
    private BaseEventConnector eventConnector;
    private BicycleEventsHelper bicycleEventsHelper;
    protected EventSourceInfo eventSourceInfo;
    private Map<String, TagEncodedMetricName> metricNameToTagEncodedMetricName = new HashMap<>();
    protected Map<String, Long> metricsMap = new HashMap<>();


    public MetricAsEventsGenerator(BicycleConfig bicycleConfig, AuthInfo bicycleAuthInfo,
                                   EventSourceInfo eventSourceInfo, JsonNode config, BaseEventConnector eventConnector) {
        this.bicycleConfig = bicycleConfig;
        this.bicycleAuthInfo = bicycleAuthInfo;
        this.config = config;
        this.eventConnector = eventConnector;
        this.bicycleEventsHelper = new BicycleEventsHelper();
        this.eventSourceInfo = eventSourceInfo;
        globalTags.put(TENANT_ID, bicycleAuthInfo.getTenantId());
        globalTags.put(CONNECTOR_ID, eventSourceInfo.getEventSourceId());
    }

    protected boolean publishMetrics(Map<String, String> attributes, Map<String, Long> metricsMap) {
        try {
            for (Map.Entry<String, Long> metricsEntry : metricsMap.entrySet()) {
                MetricUtils.getMetricRegistry().gauge(getTagEncodedMetricName(metricsEntry.getKey()).toString(),
                        () -> () -> metricsMap.get(metricsEntry.getKey()));
            }
            BicycleEvent bicycleEvent =
                    bicycleEventsHelper.createPreviewBicycleEvent(eventSourceInfo, attributes);
            BicycleEventsResult bicycleEventsResult = new BicycleEventsResult(BicycleEventList.newBuilder().build(),
                    BicycleEventList.newBuilder().addEvents(bicycleEvent).build(), Collections.EMPTY_MAP);
            eventConnector.publishEvents(bicycleAuthInfo, eventSourceInfo, bicycleEventsResult);
            logger.info("Successfully published bicycle event for metrics {}", bicycleEvent);
        } catch (Exception exception) {
            logger.error("Unable to publish metrics", exception);
        }
        return true;
    }

    private TagEncodedMetricName getTagEncodedMetricName(String metricName) {
        if (metricNameToTagEncodedMetricName.containsKey(metricName)) {
            return metricNameToTagEncodedMetricName.get(metricName);
        }

        TagEncodedMetricName encodedMetricName = TagEncodedMetricName.decode(metricName).withTags(globalTags);
        metricNameToTagEncodedMetricName.put(metricName, encodedMetricName);
        return encodedMetricName;
    }

    @Override
    public void run() {

    }
}
