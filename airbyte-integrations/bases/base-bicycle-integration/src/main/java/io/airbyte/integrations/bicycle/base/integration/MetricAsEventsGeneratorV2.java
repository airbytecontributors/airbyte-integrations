package io.airbyte.integrations.bicycle.base.integration;


import ai.apptuit.metrics.client.TagEncodedMetricName;
import ai.apptuit.ml.utils.MetricUtils;
import bicycle.io.events.proto.BicycleEvent;
import bicycle.io.events.proto.BicycleEventList;
import com.fasterxml.jackson.databind.JsonNode;
import com.inception.server.auth.model.AuthInfo;
import io.bicycle.event.mapping.utils.BicycleEventsHelper;
import io.bicycle.event.publisher.api.BicycleEventPublisher;
import io.bicycle.server.event.mapping.models.converter.BicycleEventsResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricAsEventsGeneratorV2 implements Runnable {

    private static final String TENANT_ID = "tenantId";
    protected static final String UNIQUE_IDENTIFIER = "identifier";
    protected static final String CONNECTOR_ID = "connectorId";
    protected static final String METRIC_NAME_SEPARATOR = "_";
    protected static final String EVENTS_PROCESSED_METRIC = "events_processed";
    public static final String TOTAL_EVENTS_PROCESSED_METRIC = "total_events_processed";
    protected static final String TOTAL_BYTES_PROCESSED_METRIC = "total_bytes_processed";
    protected final Logger logger = LoggerFactory.getLogger(MetricAsEventsGeneratorV2.class.getName());
    protected final Map<String, String> globalTags = new HashMap<>();
    protected BicycleConfig bicycleConfig;
    private JsonNode config;
    protected BicycleEventPublisher bicycleEventPublisher;
    protected BaseEventConnectorV2 eventConnector;
    private BicycleEventsHelper bicycleEventsHelper;
    protected EventSourceInfo eventSourceInfo;
    private Map<String, TagEncodedMetricName> metricNameToTagEncodedMetricName = new HashMap<>();
    protected Map<TagEncodedMetricName, Long> metricsMap = new HashMap<>();


    public MetricAsEventsGeneratorV2(BicycleConfig bicycleConfig, EventSourceInfo eventSourceInfo, JsonNode config, BicycleEventPublisher bicycleEventPublisher, BaseEventConnectorV2 eventConnector) {
        this.bicycleConfig = bicycleConfig;
        this.config = config;
        this.bicycleEventPublisher = bicycleEventPublisher;
        this.eventConnector = eventConnector;
        this.bicycleEventsHelper = new BicycleEventsHelper();
        this.eventSourceInfo = eventSourceInfo;
        globalTags.put(TENANT_ID, bicycleConfig.getTenantId());
        globalTags.put(CONNECTOR_ID, eventSourceInfo.getEventSourceId());
    }

    protected boolean publishMetrics(Map<String, String> attributes, Map<TagEncodedMetricName, Long> metricsMap) {
        try {
            for (Map.Entry<TagEncodedMetricName, Long> metricsEntry : metricsMap.entrySet()) {
                MetricUtils.getMetricRegistry().gauge(metricsEntry.getKey().toString(),
                        () -> () -> metricsMap.get(metricsEntry.getKey()));
            }

            AuthInfo bicycleAuthInfo = bicycleConfig.getAuthInfo();
            BicycleEvent bicycleEvent =
                    bicycleEventsHelper.createPreviewBicycleEvent(eventSourceInfo, attributes);
            BicycleEventsResult bicycleEventsResult = new BicycleEventsResult(BicycleEventList.newBuilder().build(),
                    BicycleEventList.newBuilder().addEvents(bicycleEvent).build(), Collections.EMPTY_MAP);
            bicycleEventPublisher.publishEvents(bicycleAuthInfo, eventSourceInfo, bicycleEventsResult);
            logger.info("Successfully published bicycle event for metrics {}", bicycleEvent);
        } catch (Exception exception) {
            logger.error("Unable to publish metrics", exception);
            return false;
        }
        return true;

    }

    protected TagEncodedMetricName getTagEncodedMetricName(String metricName, Map<String, String> tags) {

        tags.putAll(globalTags);

       /* if (metricNameToTagEncodedMetricName.containsKey(metricName)) {
            return metricNameToTagEncodedMetricName.get(metricName);
        }*/

        TagEncodedMetricName encodedMetricName = TagEncodedMetricName.decode(metricName).withTags(tags);
        // metricNameToTagEncodedMetricName.put(metricName, encodedMetricName);
        return encodedMetricName;
    }
//
//    private TagEncodedMetricName getTagEncodedMetricName(String metricName) {
//        if (metricNameToTagEncodedMetricName.containsKey(metricName)) {
//            return metricNameToTagEncodedMetricName.get(metricName);
//        }
//
//        TagEncodedMetricName encodedMetricName = TagEncodedMetricName.decode(metricName).withTags(globalTags);
//        metricNameToTagEncodedMetricName.put(metricName, encodedMetricName);
//        return encodedMetricName;
//    }

    public void addMetrics(Map<String, Long> metrics) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(UNIQUE_IDENTIFIER, bicycleConfig.getUniqueIdentifier());
        attributes.put(CONNECTOR_ID, eventSourceInfo.getEventSourceId());
        for(Map.Entry<String, Long> entry: metrics.entrySet()) {
            metricsMap.put(getTagEncodedMetricName(entry.getKey(), attributes), entry.getValue());
        }
    }

    @Override
    public void run() {
    }
}
