package io.airbyte.integrations.source.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.bicycle.base.integration.MetricAsEventsGenerator;
import io.bicycle.event.publisher.api.BicycleEventPublisher;
import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaMetricAsEventsGenerator extends MetricAsEventsGenerator {
    private static final String LAG_METRIC = "consumer_lag";
    private static final String TOTAL_LAG_METRIC = "total_consumer_lag";
    private static final String TOPIC = "topic";
    private static final String TOPIC_PARTITION = "topicPartition";
    private static final String CONSUMER_THREAD = "consumerThread";

    private AdminClient adminClient;
    private KafkaSourceConfig kafkaSourceConfig;
    private String consumerGroupId;
    private KafkaConsumer kafkaConsumer;

    public KafkaMetricAsEventsGenerator(BicycleConfig bicycleConfig, EventSourceInfo eventSourceInfo, JsonNode config, BicycleEventPublisher bicycleEventPublisher, KafkaSource kafkaSource) {
        super(bicycleConfig, eventSourceInfo, config, bicycleEventPublisher,kafkaSource);
        this.kafkaSourceConfig = new KafkaSourceConfig(UUID.randomUUID().toString(), config, bicycleConfig.getConnectorId(), bicycleConfig, kafkaSource.getConnectorConfigManager());
        consumerGroupId = config.has("group_id") ? config.get("group_id").asText() : null;
        kafkaConsumer = getKafkaConsumer();
        this.adminClient = getAdminClient();
    }

    private KafkaConsumer<String, String> getKafkaConsumer() {
        return kafkaSourceConfig.getMetricsConsumer();
    }

    public Map<TopicPartition, Long> getConsumerLagMetric() {

        try {
            if (adminClient == null) {
                logger.error("Admin client is null");
                return Collections.EMPTY_MAP;
            }
            Map<TopicPartition, Long> consumerOffsets = getConsumerGrpOffsets(consumerGroupId);
            Map<TopicPartition, Long> producerOffsets = getProducerOffsets(consumerOffsets);
            Map<TopicPartition, Long> lagMetrics = computeLags(consumerOffsets, producerOffsets);
            return lagMetrics;
        } catch (Exception exception) {
            logger.error("Unable to compute consumer lag metric", exception);
        }

        return Collections.EMPTY_MAP;
    }

    private AdminClient getAdminClient() {
        try {
            return AdminClient.create(kafkaSourceConfig.getAdminProperties());
        } catch (Exception exception) {
            logger.error("Unable to create admin client", exception);
        }
        return null;
    }

    private Map<TopicPartition, Long> getConsumerGrpOffsets(String groupId)
            throws ExecutionException, InterruptedException {
        if (groupId == null) {
            return Collections.EMPTY_MAP;
        }
        ListConsumerGroupOffsetsResult info = adminClient.listConsumerGroupOffsets(groupId);
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap =
                info.partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, Long> groupOffset = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : topicPartitionOffsetAndMetadataMap.entrySet()) {
            TopicPartition key = entry.getKey();
            OffsetAndMetadata metadata = entry.getValue();
            groupOffset.putIfAbsent(new TopicPartition(key.topic(), key.partition()), metadata.offset());
        }
        return groupOffset;
    }

    private Map<TopicPartition, Long> getProducerOffsets(Map<TopicPartition, Long> consumerGrpOffset) {
        List<TopicPartition> topicPartitions = new LinkedList<>();
        for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffset.entrySet()) {
            TopicPartition key = entry.getKey();
            topicPartitions.add(new TopicPartition(key.topic(), key.partition()));
        }
        return kafkaConsumer.endOffsets(topicPartitions);
    }

    private Map<TopicPartition, Long> computeLags(
            Map<TopicPartition, Long> consumerGrpOffsets,
            Map<TopicPartition, Long> producerOffsets) {
        Map<TopicPartition, Long> lags = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : consumerGrpOffsets.entrySet()) {
            Long producerOffset = producerOffsets.get(entry.getKey());
            Long consumerOffset = consumerGrpOffsets.get(entry.getKey());
            long lag = Math.abs(producerOffset - consumerOffset);
            lags.putIfAbsent(entry.getKey(), lag);
        }
        return lags;
    }

    @Override
    public void run() {
        try {
            logger.info("Starting the metrics collection");
            Map<String, String> attributes = new HashMap<>();
            attributes.put(UNIQUE_IDENTIFIER, bicycleConfig.getUniqueIdentifier());
            attributes.put(CONNECTOR_ID, eventSourceInfo.getEventSourceId());

            Map<TopicPartition, Long> consumerMetrics = getConsumerLagMetric();
            Map<String, Long> topicLag = new HashMap<>();

            for (Map.Entry<TopicPartition, Long> entry : consumerMetrics.entrySet()) {
                Map<String, String> labels = new HashMap<>();
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(LAG_METRIC);
                stringBuilder.append(METRIC_NAME_SEPARATOR);
                stringBuilder.append(entry.getKey().topic());
                stringBuilder.append(METRIC_NAME_SEPARATOR);
                labels.put(TOPIC_PARTITION, entry.getKey().topic() + "-" + entry.getKey().partition());
                labels.put(TOPIC, entry.getKey().topic());
                stringBuilder.append(entry.getKey().partition());
                attributes.put(stringBuilder.toString(), String.valueOf(entry.getValue()));
                metricsMap.put(getTagEncodedMetricName(LAG_METRIC, labels), entry.getValue());
                //  populateTagEncodedMetricName(LAG_METRIC, labels);
                if (topicLag.containsKey(entry.getKey().topic())) {
                    long value = topicLag.get(entry.getKey().topic());
                    value += entry.getValue();
                    topicLag.put(entry.getKey().topic(), value);
                } else {
                    topicLag.put(entry.getKey().topic(), entry.getValue());
                }
            }

            for (Map.Entry<String, Long> entry : topicLag.entrySet()) {
                Map<String, String> labels = new HashMap<>();
                String metricName = TOTAL_LAG_METRIC + METRIC_NAME_SEPARATOR + entry.getKey();
                attributes.put(metricName, String.valueOf(entry.getValue()));
                labels.put(TOPIC, entry.getKey());
                metricsMap.put(getTagEncodedMetricName(TOTAL_LAG_METRIC, labels), entry.getValue());
                getTagEncodedMetricName(TOTAL_LAG_METRIC, labels);
            }

            Map<String, Map<String, Long>> consumerThreadToTopicPartitionMessagesRead =
                    ((KafkaSource) eventConnector).getTopicPartitionRecordsRead();


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
            this.publishMetrics(attributes,metricsMap);

        } catch (Exception exception) {
            logger.error("Unable to publish metrics", exception);
        }
    }

}
