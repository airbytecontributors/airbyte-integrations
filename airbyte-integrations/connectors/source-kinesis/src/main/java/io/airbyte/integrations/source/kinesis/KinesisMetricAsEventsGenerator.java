package io.airbyte.integrations.source.kinesis;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.bicycle.base.integration.MetricAsEventsGenerator;
import io.bicycle.event.publisher.api.BicycleEventPublisher;
import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.model.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class KinesisMetricAsEventsGenerator extends MetricAsEventsGenerator {
    private static final String LAG_METRIC = "consumer_lag";
    private static final String TOTAL_LAG_METRIC = "total_consumer_lag";
    private static final String TOPIC = "topic";
    private static final String TOPIC_PARTITION = "topicPartition";
    private static final String CONSUMER_THREAD = "consumerThread";

    private static final String MIILIS_BEHIND_LATEST_METRIC = "MillisBehindLatest";
    private static final String STAT_TYPE = "Sum";
    private final CloudWatchAsyncClient cloudWatchClient;
    private final String applicationName;
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisMetricAsEventsGenerator.class);
    private final ArrayList<MetricDataQuery> metricDataQueries;
    private final KinesisClientConfig kinesisClientConfig;
    private Instant lastQueryTime=Instant.now().truncatedTo(ChronoUnit.MILLIS);
    private final int period=300;
    private Map<String, Metric> idToshardId = new HashMap<>();
    public KinesisMetricAsEventsGenerator(BicycleConfig bicycleConfig, EventSourceInfo eventSourceInfo, JsonNode config, BicycleEventPublisher bicycleEventPublisher, KinesisSource kinesisSource) {
        super(bicycleConfig, eventSourceInfo, config, bicycleEventPublisher, kinesisSource);
        applicationName = ((KinesisSource) eventConnector).getApplicationName();
        kinesisClientConfig = ((KinesisSource) eventConnector).kinesisClientConfig;
        cloudWatchClient = ((KinesisSource) eventConnector).kinesisClientConfig.getCloudWatchClient();
        metricDataQueries = new ArrayList<MetricDataQuery>();

        try {
            DimensionFilter dimensionFilter = DimensionFilter.builder().name("ShardId").build();
            ListMetricsRequest getRequiredMetricListRequest = ListMetricsRequest.builder().metricName(MIILIS_BEHIND_LATEST_METRIC).namespace(applicationName).dimensions(dimensionFilter).build();
            CompletableFuture<ListMetricsResponse> listMetricsResponseCompletableFuture = cloudWatchClient.listMetrics(getRequiredMetricListRequest);
            ListMetricsResponse listMetricsResponse = listMetricsResponseCompletableFuture.get();

            int i = 0;
            for (Metric metric : listMetricsResponse.metrics()) {
                i++;
                idToshardId.put("m"+i, metric);
                MetricDataQuery metricDataQuery = MetricDataQuery.builder().id("m"+i).metricStat(MetricStat.builder().metric(metric).stat(STAT_TYPE).period(period).build()).build();
                metricDataQueries.add(metricDataQuery);
            }
        }
        catch (Exception e) {
            LOGGER.error("Error getting Metric Names for ", e);
        }

    }

    private Map<Metric, Double> getClientLagMetric() {
        Map<Metric, Double> metricToValue = new HashMap<>();
        try {
            GetMetricDataRequest getMetricDataRequest = GetMetricDataRequest.builder().metricDataQueries(metricDataQueries).endTime(Instant.now().truncatedTo(ChronoUnit.MILLIS)).startTime(lastQueryTime).build();
            CompletableFuture<GetMetricDataResponse> getMetricDataResponseCompletableFuture = cloudWatchClient.getMetricData(getMetricDataRequest);
            GetMetricDataResponse getMetricDataResponse = getMetricDataResponseCompletableFuture.get();
            for(MetricDataResult metricDataResult: getMetricDataResponse.metricDataResults()) {
                if (metricDataResult.values().isEmpty() == false) {
                    LOGGER.info("id {} values {}", metricDataResult.id(), metricDataResult.values());
                    metricToValue.put(idToshardId.get(metricDataResult.id()),metricDataResult.values().get(0));
                }
            }
            this.lastQueryTime=Instant.now().truncatedTo(ChronoUnit.MILLIS);;
            return metricToValue;
        } catch (Exception e) {
            LOGGER.error("Error getting Metric from Cloud watch", e);
        }
        return Collections.EMPTY_MAP;
    }

    @Override
    public void run() {
        try {
            logger.info("Starting the metrics collection");
            Map<String, String> attributes = new HashMap<>();
            attributes.put(UNIQUE_IDENTIFIER, bicycleConfig.getUniqueIdentifier());
            attributes.put(CONNECTOR_ID, eventSourceInfo.getEventSourceId());

            Map<Metric, Double> consumerMetrics = getClientLagMetric();
            Map<String, Long> topicLag = new HashMap<>();

            for (Map.Entry<Metric, Double> entry : consumerMetrics.entrySet()) {
                Map<String, String> labels = new HashMap<>();
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(LAG_METRIC);
                stringBuilder.append(METRIC_NAME_SEPARATOR);
                stringBuilder.append(kinesisClientConfig.getConfiguredStream());
                stringBuilder.append(METRIC_NAME_SEPARATOR);
                labels.put(TOPIC_PARTITION, kinesisClientConfig.getConfiguredStream() + "-" + entry.getKey().dimensions().get(0).value());
                labels.put(TOPIC, kinesisClientConfig.getConfiguredStream());
                stringBuilder.append(entry.getKey().dimensions().get(0).value());
                attributes.put(stringBuilder.toString(), String.valueOf(entry.getValue()));
                metricsMap.put(getTagEncodedMetricName(LAG_METRIC, labels), entry.getValue().longValue());
                //  populateTagEncodedMetricName(LAG_METRIC, labels);
                if (topicLag.containsKey(kinesisClientConfig.getConfiguredStream())) {
                    long value = topicLag.get(kinesisClientConfig.getConfiguredStream());
                    value += entry.getValue();
                    topicLag.put(kinesisClientConfig.getConfiguredStream(), value);
                } else {
                    topicLag.put(kinesisClientConfig.getConfiguredStream(), entry.getValue().longValue());
                }
            }

            for (Map.Entry<String, Long> entry : topicLag.entrySet()) {
                Map<String, String> labels = new HashMap<>();
                String metricName = TOTAL_LAG_METRIC + METRIC_NAME_SEPARATOR + entry.getKey();
                attributes.put(metricName, String.valueOf(entry.getValue()));
                labels.put(TOPIC, entry.getKey());
                metricsMap.put(getTagEncodedMetricName(CONNECTOR_LAG_STRING, labels), entry.getValue());
                getTagEncodedMetricName(CONNECTOR_LAG_STRING, labels);
            }

            Map<String, Map<String, Long>> consumerThreadToTopicPartitionMessagesRead =
                    ((KinesisSource) eventConnector).getClientToStreamShardRecordsRead();


            Long totalRecordsConsumed = 0L;

            for (Map.Entry<String, Map<String, Long>> consumerThreadEntry :
                    consumerThreadToTopicPartitionMessagesRead.entrySet()) {
                for (Map.Entry<String, Long> entry : consumerThreadEntry.getValue().entrySet()) {
                    Map<String, String> labels = new HashMap<>();
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append(EVENTS_PROCESSED_METRIC);
                    stringBuilder.append(METRIC_NAME_SEPARATOR);
                    stringBuilder.append(entry.getKey());
                    attributes.put(stringBuilder.toString(), String.valueOf(entry.getValue()));
                    labels.put(CONSUMER_THREAD, consumerThreadEntry.getKey());
                    labels.put(TOPIC_PARTITION, entry.getKey());
                    metricsMap.put(getTagEncodedMetricName(EVENTS_PROCESSED_METRIC, labels), entry.getValue());
                    //  populateTagEncodedMetricName(EVENTS_PROCESSED_METRIC, labels);
                    totalRecordsConsumed += entry.getValue();
                }
            }

            attributes.put(TOTAL_EVENTS_PROCESSED_METRIC, String.valueOf(totalRecordsConsumed));
            metricsMap.put(getTagEncodedMetricName(TOTAL_EVENTS_PROCESSED_METRIC, new HashMap<>()),
                    totalRecordsConsumed);


//            Map<Metric, Double> clientLagMetric = getClientLagMetric();
//            Map<String, Long> shardLag = new HashMap<>();
//            for (Map.Entry<Metric, Double> entry : clientLagMetric.entrySet()) {
//                StringBuilder stringBuilder = new StringBuilder();
//                stringBuilder.append(LAG_METRIC);
//                stringBuilder.append(METRIC_NAME_SEPARATOR);
//                stringBuilder.append(kinesisClientConfig.getConfiguredStream());
//                stringBuilder.append(METRIC_NAME_SEPARATOR);
//                stringBuilder.append(entry.getKey().dimensions().get(0).value());
//                attributes.put(stringBuilder.toString(), String.valueOf(entry.getValue()));
//                metricsMap.put(stringBuilder.toString(), entry.getValue().longValue());
//
//                if (shardLag.containsKey(kinesisClientConfig.getConfiguredStream())) {
//                    long value = shardLag.get(kinesisClientConfig.getConfiguredStream());
//                    value += entry.getValue();
//                    shardLag.put(kinesisClientConfig.getConfiguredStream(), value);
//                } else {
//                    shardLag.put(kinesisClientConfig.getConfiguredStream(), entry.getValue().longValue());
//                }
//            }
//
//            for (Map.Entry<String, Long> entry : shardLag.entrySet()) {
//                String metricName = TOTAL_LAG_METRIC + METRIC_NAME_SEPARATOR + entry.getKey();
//                attributes.put(metricName, String.valueOf(entry.getValue()));
//                metricsMap.put(metricName, entry.getValue());
//            }
//
//            Map<String, Map<String, Long>> clientToStreamShardRecordsRead =
//                    ((KinesisSource) this.eventConnector).getClientToStreamShardRecordsRead();
//
//
//            for (Map.Entry<String, Map<String, Long>> clientThreadEntry :
//                    clientToStreamShardRecordsRead.entrySet()) {
//                for (Map.Entry<String, Long> entry : clientThreadEntry.getValue().entrySet()) {
//                    StringBuilder stringBuilder = new StringBuilder();
//                    stringBuilder.append(EVENTS_PROCESSED_METRIC);
//                    stringBuilder.append(METRIC_NAME_SEPARATOR);
//                    stringBuilder.append(entry.getKey());
//                    attributes.put(stringBuilder.toString(), String.valueOf(entry.getValue()));
//                    metricsMap.put(stringBuilder.toString(), entry.getValue());
//                    totalRecordsConsumed += entry.getValue();
//                }
//            }
//
//            attributes.put(TOTAL_EVENTS_PROCESSED_METRIC, String.valueOf(totalRecordsConsumed));
//            metricsMap.put(TOTAL_EVENTS_PROCESSED_METRIC, totalRecordsConsumed);
            this.publishMetrics(attributes,metricsMap);
        } catch (Exception exception) {
            logger.error("Unable to publish metrics", exception);
        }
    }
}
