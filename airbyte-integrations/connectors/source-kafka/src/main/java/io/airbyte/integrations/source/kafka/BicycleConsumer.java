package io.airbyte.integrations.source.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.integrations.base.Command;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;

import io.bicycle.integration.connector.runtime.BackFillConfiguration;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.integration.common.utils.CommonUtil;
import io.bicycle.integration.common.writer.Writer;
import io.bicycle.integration.common.writer.WriterFactory;
import io.bicycle.integration.connector.SyncDataRequest;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.airbyte.integrations.source.kafka.KafkaSource.STREAM_NAME;

/**
 */
public class BicycleConsumer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(BicycleConsumer.class.getName());
    private final KafkaSourceConfig kafkaSourceConfig;
    private final JsonNode config;
    private final BicycleConfig bicycleConfig;
    private final Map<String, Long> topicPartitionRecordsRead;
    private final String name;
    private final ConfiguredAirbyteCatalog catalog;
    private EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier;
    private final KafkaSource kafkaSource;
    private final EventSourceInfo eventSourceInfo;
    private final boolean isDestinationSyncConnector;
    private final SyncDataRequest syncDataRequest;
    private final BackFillConfiguration backFillConfiguration;

    public BicycleConsumer(String name, Map<String, Long> topicPartitionRecordsRead, BicycleConfig bicycleConfig, JsonNode connectorConfig, ConfiguredAirbyteCatalog configuredCatalog, EventSourceInfo eventSourceInfo, EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier, KafkaSource instance) {
        this(name, topicPartitionRecordsRead, bicycleConfig, connectorConfig, configuredCatalog, eventSourceInfo,
                eventConnectorJobStatusNotifier, instance, false, null);
    }

    public BicycleConsumer(String name,
                           Map<String, Long> topicPartitionRecordsRead,
                           BicycleConfig bicycleConfig,
                           JsonNode connectorConfig,
                           ConfiguredAirbyteCatalog configuredCatalog,
                           EventSourceInfo eventSourceInfo,
                           EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier,
                           KafkaSource instance,
                           boolean isDestinationSyncConnector,
                           SyncDataRequest syncDataRequest) {
        this.name = name;
        this.config = connectorConfig;
        this.catalog = configuredCatalog;
        this.kafkaSource = instance;
        this.backFillConfiguration = kafkaSource.getRuntimeConfig() == null ? BackFillConfiguration.getDefaultInstance() : kafkaSource.getRuntimeConfig().getBackFillConfig();
        boolean isBackFillEnabled = backFillConfiguration.getEnableBackFill();
        logger.info("Runtime config available is {}", kafkaSource.getRuntimeConfig());
        if (isBackFillEnabled) {
            logger.info("Backfill is enabled for connector {}, setting auto_offset_reset to earliest",
                    bicycleConfig.getConnectorId());
            ((ObjectNode) config).put("auto_offset_reset", "earliest");
        }

        this.kafkaSourceConfig = new KafkaSourceConfig(name, config, getConnectorId(catalog), bicycleConfig,
                instance.getConnectorConfigManager());
        this.bicycleConfig = bicycleConfig;
        this.topicPartitionRecordsRead = topicPartitionRecordsRead;
        this.eventConnectorJobStatusNotifier = eventConnectorJobStatusNotifier;

        this.eventSourceInfo = eventSourceInfo;
        this.isDestinationSyncConnector = isDestinationSyncConnector;
        this.syncDataRequest = syncDataRequest;
        logger.info("Initialized consumer thread with name {}", name);
    }

    @Override
    public void run() {
        int retry = config.has("repeated_calls") ? config.get("repeated_calls").intValue() : 1;
        int failed = 0;
        while (failed <= retry) {
            try {
                if (isDestinationSyncConnector) {
                    Writer writer = WriterFactory.getWriter(syncDataRequest.getSyncDestination());
                    syncData(bicycleConfig, config, catalog, null, syncDataRequest, writer);
                } else {
                    // read completed means we are manually stopping connector
                    read(bicycleConfig, config, catalog, null);
                }
                return;
            } catch (Exception exception) {
                int retryLeft = retry - failed;
                logger.error("Unable to run consumer with config " + config + ", retryleft - " + retryLeft,
                        exception);
                failed++;
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {

                }
            }
        }
        if (eventConnectorJobStatusNotifier != null
                && eventConnectorJobStatusNotifier.getNumberOfThreadsRunning().decrementAndGet() <= 0) {
            eventConnectorJobStatusNotifier.getSchedulesExecutorService().shutdown();
            eventConnectorJobStatusNotifier.removeConnectorInstanceFromMap(eventSourceInfo.getEventSourceId());
            AuthInfo authInfo = bicycleConfig.getAuthInfo();
            eventConnectorJobStatusNotifier.sendStatus(JobExecutionStatus.failure,"Shutting down the kafka Event Connector", eventSourceInfo.getEventSourceId(), this.kafkaSource.getTotalRecordsConsumed(), authInfo);
        }
        logger.info("All the retries failed, exiting the thread for consumer {}",name);
    }

    public int getNumberOfRecordsToBeReturnedBasedOnSamplingRate(int noOfRecords, int samplingRate) {
        int value = ((noOfRecords * samplingRate) / 100);
        if (value == 0) {
            return 1;
        }
        return value;
    }

    public void read(BicycleConfig bicycleConfig, final JsonNode config, final ConfiguredAirbyteCatalog configuredAirbyteCatalog, final JsonNode state) throws InterruptedException {
        final boolean check = check(config);

        logger.info("======Starting read operation for consumer " + name + " config: " + config + " catalog:"+ configuredAirbyteCatalog + "=======");
        if (!check) {
            throw new RuntimeException("Unable establish a connection");
        }

        String topic = config.get(STREAM_NAME).asText();
        if (topic == null || topic.isEmpty()) {
            topic = config.has("test_topic") ? config.get("test_topic").asText() : "";
            ((ObjectNode) config).put(STREAM_NAME, topic);
        }

        logger.info("Reading from topic {} for connector {}", topic, bicycleConfig.getConnectorId());

        final KafkaConsumer<String, JsonNode> consumer = kafkaSourceConfig.getConsumer(Command.READ);

        boolean resetOffsetToLatest = config.has("reset_to_latest") ?
                Boolean.parseBoolean(config.get("reset_to_latest").asText()) : Boolean.FALSE;

        if (resetOffsetToLatest) {
            resetOffsetsToLatest(consumer, topic);
        }

        int samplingRate = config.has("sampling_rate") ? config.get("sampling_rate").asInt() : 100;
        if (kafkaSource.getRuntimeConfig() != null) {
            samplingRate = kafkaSource.getRuntimeConfig().getEventsSamplingRate();
        }

        BackFillConfiguration backfillConfiguration = kafkaSource.getRuntimeConfig().getBackFillConfig();

        int sampledRecords = 0;
        try {
            while (!this.kafkaSource.getStopConnectorBoolean().get()) {
                final List<ConsumerRecord<String, JsonNode>> recordsList = new ArrayList<>();
                final ConsumerRecords<String, JsonNode> consumerRecords =
                        consumer.poll(Duration.of(5000, ChronoUnit.MILLIS));
                int counter = 0;
                logger.debug("No of records actually read by consumer {} are {}", name, consumerRecords.count());
                sampledRecords = getNumberOfRecordsToBeReturnedBasedOnSamplingRate(consumerRecords.count(), samplingRate);

                for (ConsumerRecord record : consumerRecords) {
                    logger.debug("Consumer Record: key - {}, value - {}, partition - {}, offset - {}",
                            record.key(), record.value(), record.partition(), record.offset());

                    long timestamp = record.timestamp();
                    //In case of backfill we need to only consume message that fall in backfill timestamp range
                    if (!kafkaSource.shouldContinue(backfillConfiguration, timestamp)) {
                        continue;
                    }

                    if (counter > sampledRecords) {
                        break;
                    }
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append(record.topic());
                    stringBuilder.append("_");
                    stringBuilder.append(record.partition());
                    String key = stringBuilder.toString();
                    if (topicPartitionRecordsRead.containsKey(key)) {
                        Long value = topicPartitionRecordsRead.get(key);
                        value = value + 1;
                        topicPartitionRecordsRead.put(key, value);
                    } else {
                        topicPartitionRecordsRead.put(key, 1L);
                    }
                    recordsList.add(record);
                    counter++;
                }

                logger.info("No of records read from consumer after sampling {} are {} ", name,
                        counter);

                if (recordsList.size() == 0) {
                    continue;
                }

                EventProcessorResult eventProcessorResult = null;
                AuthInfo authInfo = bicycleConfig.getAuthInfo();
                try {
                    List<RawEvent> rawEvents = this.kafkaSource.convertRecordsToRawEvents(recordsList);
                    eventProcessorResult = this.kafkaSource.convertRawEventsToBicycleEvents(authInfo,eventSourceInfo,rawEvents);
                } catch (Exception exception) {
                    logger.error("Unable to convert raw records to bicycle events for {} ",name, exception);
                }

                try {
                    this.kafkaSource.publishEvents(authInfo, eventSourceInfo, eventProcessorResult);
                    consumer.commitAsync();
                } catch (Exception exception) {
                    logger.error("Unable to publish bicycle events for {} ", name, exception);
                }
                int delayInSecs = this.kafkaSource.getDelayInProcessing(backfillConfiguration);
                if (delayInSecs > 0) {
                    logger.info("Connector Id {} going to sleep for {} ms at timestamp {}",
                            eventSourceInfo.getEventSourceId(), delayInSecs, Instant.now());
                    Thread.sleep(delayInSecs);
                    logger.info("Connector Id {} Waking up at at timestamp {}",
                            eventSourceInfo.getEventSourceId(), Instant.now());
                }
            }

        } finally {
            consumer.close();
            kafkaSourceConfig.resetConsumer();
        }
    }
    public void syncData(BicycleConfig bicycleConfig,
                         final JsonNode config,
                         final ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                         final JsonNode state,
                         final SyncDataRequest syncDataRequest,
                         final Writer writer) {

        if (syncDataRequest == null) {
            throw new RuntimeException("Sync data request cannot be null");
        } validateRequest(syncDataRequest);

        final boolean check = check(config);
        String traceInfo = CommonUtil.getTraceInfo(syncDataRequest.getTraceInfo());

        logger.info(traceInfo + " Starting read operation for consumer " + name + " config: " + config + " " +
                "catalog: "+ configuredAirbyteCatalog + " Sync data request: " + syncDataRequest);
        if (!check) {
            throw new RuntimeException("Unable establish a connection");
        }

        final KafkaConsumer<String, JsonNode> consumer = kafkaSourceConfig.getConsumer(Command.READ);

        boolean resetOffsetToLatest = config.has("reset_to_latest") ?
                Boolean.parseBoolean(config.get("reset_to_latest").asText()) : Boolean.FALSE;

        if (resetOffsetToLatest) {
            String topic = config.get(STREAM_NAME).asText();
            resetOffsetsToLatest(consumer, topic);
        }

        int samplingRate = config.has("sampling_rate") ? config.get("sampling_rate").asInt(): 100;

        int sampledRecords = 0;
        long totalEventsSynced = 0;
        try {
            while (!this.kafkaSource.getStopConnectorBoolean().get()) {
                final List<ConsumerRecord<String, JsonNode>> recordsList = new ArrayList<>();
                final ConsumerRecords<String, JsonNode> consumerRecords =
                        consumer.poll(Duration.of(5000, ChronoUnit.MILLIS));
                int counter = 0;
                logger.info(traceInfo + " No of records actually read by consumer {} are {}", name,
                        consumerRecords.count());
                sampledRecords = getNumberOfRecordsToBeReturnedBasedOnSamplingRate(consumerRecords.count(), samplingRate);

                for (ConsumerRecord record : consumerRecords) {
                    logger.debug(traceInfo + " Consumer Record: key - {}, value - {}, partition - {}, offset - {}",
                            record.key(), record.value(), record.partition(), record.offset());

                    if (counter > sampledRecords) {
                        break;
                    }
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append(record.topic());
                    stringBuilder.append("_");
                    stringBuilder.append(record.partition());
                    String key = stringBuilder.toString();
                    if (topicPartitionRecordsRead.containsKey(key)) {
                        Long value = topicPartitionRecordsRead.get(key);
                        value = value + 1;
                        topicPartitionRecordsRead.put(key, value);
                    } else {
                        topicPartitionRecordsRead.put(key, 1L);
                    }
                    recordsList.add(record);
                    counter++;
                }

                logger.info(traceInfo + " No of records sampled for consumer {} are {} ", name, counter);

                if (recordsList.size() == 0) {
                    continue;
                }
                boolean isLast = totalEventsSynced + recordsList.size() >= syncDataRequest.getSyncDataCountLimit();

                List<RawEvent> rawEvents = this.kafkaSource.convertRecordsToRawEvents(recordsList);
                AuthInfo authInfo = bicycleConfig.getAuthInfo();
                this.kafkaSource.processAndSync(
                        authInfo,
                        traceInfo,
                        syncDataRequest.getConfiguredConnectorStream().getConfiguredConnectorStreamId(),
                        eventSourceInfo,
                        System.currentTimeMillis(),
                        writer,
                        rawEvents,
                        true
                );

                try {
                    consumer.commitAsync();
                } catch (Exception e) {
                    logger.error("Unable to commit to kafka " + name, e);
                }

                totalEventsSynced += recordsList.size();
                logger.info(traceInfo + " No of records processed and synced from consumer {} are {} ", name,
                        totalEventsSynced);

                if (isLast) {
                    logger.info(traceInfo + " Completed data sync. Total events synced: {}", totalEventsSynced);
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("{}, Exception in bicycle consumer {}", traceInfo, name, e);
        } finally {
            logger.warn("{}, Closing the consumer {}", traceInfo, name);
            consumer.close();
            kafkaSourceConfig.resetConsumer();
        }
    }

    public boolean check(final JsonNode config) {
        KafkaConsumer<String, JsonNode> consumer = null;
        try {
            final String testTopic = config.has("test_topic") ? config.get("test_topic").asText() : "";
            if (!testTopic.isBlank()) {
                consumer = kafkaSourceConfig.getCheckConsumer();
                consumer.subscribe(Pattern.compile(testTopic));
                consumer.listTopics();
                logger.info("Successfully connected to Kafka brokers for topic '{}'.",
                        config.get("test_topic").asText());
            }
            return true;
        } catch (final Exception e) {
            logger.error("Exception attempting to connect to the Kafka brokers: ", e);
            return false;
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    private void resetOffsetsToLatest(KafkaConsumer kafkaConsumer, String topicName) {
        try {
            if (topicName == null) {
                return;
            }
            kafkaConsumer.poll(Duration.ZERO);
            List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topicName);

            for (PartitionInfo partitionInfo : partitionInfos) {
                TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                kafkaConsumer.seekToEnd(Collections.singletonList(topicPartition));
            }
            logger.info("Offset reset to latest for topic {} ", topicName);
        } catch (Exception exception) {
            logger.error("Unable to reset offsets to latest", exception);
        }
    }

    private String getConnectorId(ConfiguredAirbyteCatalog catalog) {
        Map<String, Object> additionalProperties = catalog.getAdditionalProperties();
        return additionalProperties.containsKey("bicycleConnectorId")
                ? additionalProperties.get("bicycleConnectorId").toString() : null;
    }

    private void validateRequest(SyncDataRequest syncDataRequest) {
        if (syncDataRequest.getSyncDataCountLimit() == 0) {
            throw new RuntimeException("Limit cannot be null");
        }
    }
}
