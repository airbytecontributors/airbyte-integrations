package io.airbyte.integrations.source.pubsub;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.Metric;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.monitoring.v3.MetricServiceSettings;
import com.google.monitoring.v3.*;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import io.airbyte.integrations.bicycle.base.integration.BicycleConfig;
import io.airbyte.integrations.bicycle.base.integration.MetricAsEventsGenerator;
import io.bicycle.event.publisher.api.BicycleEventPublisher;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.google.protobuf.util.Timestamps.fromMillis;
import static java.lang.System.currentTimeMillis;

public class PubsubMetricAsEventsGenerator extends MetricAsEventsGenerator {
    private static final String LAG_METRIC = "consumer_lag";
    private static final String TOTAL_LAG_METRIC = "total_consumer_lag";
    private static final String TOPIC = "topic";
    private static final String CONSUMER_THREAD = "consumerThread";

    private static final String MIILIS_BEHIND_LATEST_METRIC = "MillisBehindLatest";
    private static final String STAT_TYPE = "Sum";
    private static final Logger LOGGER = LoggerFactory.getLogger(PubsubMetricAsEventsGenerator.class);
    private final PubsubSourceConfig pubsubSourceConfig;
    private MetricServiceClient metricServiceClient;
    private final ProjectSubscriptionName projectSubscriptionName;
    private ProjectName projectName;
    private Instant lastQueryTime=Instant.now().truncatedTo(ChronoUnit.MILLIS);
    private final int period=300;
    public PubsubMetricAsEventsGenerator(BicycleConfig bicycleConfig, EventSourceInfo eventSourceInfo, JsonNode config, BicycleEventPublisher bicycleEventPublisher, PubsubSource pubsubSource) {
        super(bicycleConfig, eventSourceInfo, config, bicycleEventPublisher, pubsubSource);
        pubsubSourceConfig = ((PubsubSource) eventConnector).getPubsubSourceConfig();
        projectName = pubsubSourceConfig.getProjectName();
        FixedCredentialsProvider gcpCredentials = pubsubSourceConfig.getGCPCredentials();
        projectSubscriptionName = pubsubSourceConfig.getProjectSubscriptionName();
        try {
            metricServiceClient = MetricServiceClient.create(MetricServiceSettings.newBuilder().setCredentialsProvider(gcpCredentials).build());
        } catch (IOException e) {
            logger.error("Unable to create Metric Service Client for pubsub connector with connector Id {}", bicycleConfig.getConnectorId(), e);
        }

    }


    private Map<Metric, Double> getClientLagMetric() {
        Map<Metric, Double> metricToValue = new HashMap<>();
        try {
            TimeInterval interval = TimeInterval.newBuilder()
                    .setStartTime(fromMillis(currentTimeMillis() - (120 * 1000)))
                    .setEndTime(fromMillis(currentTimeMillis()))
                    .build();

            ListTimeSeriesRequest request = ListTimeSeriesRequest.newBuilder().setName(projectName.toString())
                    .setFilter("metric.type=\"pubsub.googleapis.com/subscription/num_undelivered_messages\"")
                    .setInterval(interval)
                    .setView(ListTimeSeriesRequest.TimeSeriesView.FULL)
                    .build();

            MetricServiceClient.ListTimeSeriesPagedResponse listTimeSeriesPagedResponse = metricServiceClient.listTimeSeries(request);
            for (TimeSeries timeSeries : listTimeSeriesPagedResponse.iterateAll()) {
                Map<String, String> labelsMap = timeSeries.getResource().getLabelsMap();
                if (labelsMap.get("subscription_id").equals(projectSubscriptionName.getSubscription())) {
                    Point point = timeSeries.getPointsList().get(0);
                    Double value = getValue(point.getValue());
                    metricToValue.put(timeSeries.getMetric(), value);
                }
            }
            return metricToValue;
        } catch (Exception e) {
            LOGGER.error("Error getting Metric from GCP", e);
        }
        return Collections.EMPTY_MAP;
    }

    public Double getValue(TypedValue value) {
        switch (value.getValueCase()) {
            case DOUBLE_VALUE:
                return value.getDoubleValue();
            default:
                return Double.valueOf(value.getInt64Value());
        }
    }

    @Override
    public void run() {
        try {
            logger.info("Starting the metrics collection");
            Map<String, String> attributes = new HashMap<>();
            attributes.put(UNIQUE_IDENTIFIER, bicycleConfig.getUniqueIdentifier());
            attributes.put(CONNECTOR_ID, eventSourceInfo.getEventSourceId());

            Map<Metric, Double> consumerMetrics = getClientLagMetric();
            ProjectSubscriptionName projectSubscriptionName = pubsubSourceConfig.getProjectSubscriptionName();
            Long totalLag = 0L;
            for (Map.Entry<Metric, Double> entry : consumerMetrics.entrySet()) {
                Map<String, String> labels = new HashMap<>();
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(LAG_METRIC);
                stringBuilder.append(METRIC_NAME_SEPARATOR);
                stringBuilder.append(projectSubscriptionName.getSubscription());
                stringBuilder.append(METRIC_NAME_SEPARATOR);
                labels.put(TOPIC, projectSubscriptionName.getSubscription());
                String[] splitMetricTypeString = entry.getKey().getType().split("/");
                String metricName = splitMetricTypeString[splitMetricTypeString.length - 1];
                stringBuilder.append(metricName);
                totalLag += entry.getValue().longValue();
                attributes.put(stringBuilder.toString(), String.valueOf(entry.getValue()));
                metricsMap.put(getTagEncodedMetricName(LAG_METRIC, labels), entry.getValue().longValue());
            }

            Map<String, Long> consumerThreadToTopicPartitionMessagesRead =
                    ((PubsubSource) eventConnector).getConsumerToSubscriptionRecordsRead();


            Long totalRecordsConsumed = 0L;
            Map<String, String> labelForTotalLagMetrics = new HashMap<>();
            String metricName = TOTAL_LAG_METRIC + METRIC_NAME_SEPARATOR + projectSubscriptionName.getSubscription();
            attributes.put(metricName, String.valueOf(totalLag));
            labelForTotalLagMetrics.put(TOPIC, projectSubscriptionName.getSubscription());
            metricsMap.put(getTagEncodedMetricName(TOTAL_LAG_METRIC, labelForTotalLagMetrics), totalLag);
            getTagEncodedMetricName(TOTAL_LAG_METRIC, labelForTotalLagMetrics);

            for (Map.Entry<String, Long> entry : consumerThreadToTopicPartitionMessagesRead.entrySet()) {
                Map<String, String> labels = new HashMap<>();
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(EVENTS_PROCESSED_METRIC);
                stringBuilder.append(METRIC_NAME_SEPARATOR);
                stringBuilder.append(projectSubscriptionName.getSubscription());
                attributes.put(stringBuilder.toString(), String.valueOf(entry.getValue()));
                labels.put(CONSUMER_THREAD, entry.getKey());
                metricsMap.put(getTagEncodedMetricName(EVENTS_PROCESSED_METRIC, labels), entry.getValue());
                //  populateTagEncodedMetricName(EVENTS_PROCESSED_METRIC, labels);
                totalRecordsConsumed += entry.getValue();
            }

            attributes.put(TOTAL_BYTES_PROCESSED_METRIC, String.valueOf
                    (((PubsubSource) eventConnector).getTotalBytesProcessed()));
            metricsMap.put(getTagEncodedMetricName(TOTAL_BYTES_PROCESSED_METRIC, new HashMap<>()),
                    ((PubsubSource) eventConnector).getTotalBytesProcessed().get());
            attributes.put(TOTAL_EVENTS_PROCESSED_METRIC, String.valueOf(totalRecordsConsumed));
            metricsMap.put(getTagEncodedMetricName(TOTAL_EVENTS_PROCESSED_METRIC, new HashMap<>()),
                    totalRecordsConsumed);
            this.publishMetrics(attributes, metricsMap);
        } catch (Exception exception) {
            logger.error("Unable to publish metrics", exception);
        }
    }
}
