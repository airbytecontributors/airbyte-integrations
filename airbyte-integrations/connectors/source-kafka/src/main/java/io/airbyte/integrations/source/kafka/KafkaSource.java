/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.inception.schema.types.PropertyValue;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.base.Command;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.BicycleConfig;
import io.airbyte.integrations.bicycle.base.integration.CommonUtils;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.integrations.bicycle.base.integration.MetricAsEventsGenerator;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.SyncMode;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.integration.common.utils.CommonUtil;
import io.bicycle.integration.connector.SyncDataRequest;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSource extends BaseEventConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);
  public static final String STREAM_NAME = "stream_name";
  private static final int CONSUMER_THREADS_DEFAULT_VALUE = 1;
  protected AtomicBoolean stopConnectorBoolean = new AtomicBoolean(false);
  private final Map<String, Map<String, Long>> consumerToTopicPartitionRecordsRead = new HashMap<>();

  public KafkaSource(SystemAuthenticator systemAuthenticator, EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier) {
    super(systemAuthenticator,eventConnectorJobStatusNotifier);
  }

  protected AtomicBoolean getStopConnectorBoolean() {
    return stopConnectorBoolean;
  }

  @Override
  public AirbyteConnectionStatus check(final JsonNode config) {
    KafkaSourceConfig kafkaSourceConfig = new KafkaSourceConfig(UUID.randomUUID().toString(), config, null);
    KafkaConsumer<String, JsonNode> consumer = null;
    try {
      final String testTopic = config.has("test_topic") ? config.get("test_topic").asText() : "";
      if (!testTopic.isBlank()) {
        consumer = kafkaSourceConfig.getCheckConsumer();
        consumer.subscribe(Pattern.compile(testTopic));
        consumer.listTopics();
        LOGGER.info("Successfully connected to Kafka brokers for topic '{}'.", config.get("test_topic").asText());
      }
      return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);
    } catch (final Exception e) {
      LOGGER.error("Exception attempting to connect to the Kafka brokers: ", e);
      return new AirbyteConnectionStatus()
          .withStatus(Status.FAILED)
          .withMessage("Could not connect to the Kafka brokers with provided configuration. \n" + e.getMessage());
    } finally {
      if (consumer != null) {
        consumer.close();
      }
    }
  }

  @Override
  public AirbyteCatalog discover(final JsonNode config) {
    KafkaSourceConfig kafkaSourceConfig = new KafkaSourceConfig(UUID.randomUUID().toString(), config, null);
    KafkaConsumer<String, JsonNode> consumer = kafkaSourceConfig.getConsumer(Command.DISCOVER);
    final Set<String> topicsToSubscribe = kafkaSourceConfig.getTopicsToSubscribe();
    final List<AirbyteStream> streams = topicsToSubscribe.stream().map(topic -> CatalogHelpers
        .createAirbyteStream(topic, Field.of("value", JsonSchemaType.STRING))
        .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL)))
        .collect(Collectors.toList());
    consumer.close();
    return new AirbyteCatalog().withStreams(streams);
  }

  public void stopEventConnector() {
    stopConnectorBoolean.set(true);
    super.stopEventConnector("Kafka Event Connector Stopped manually", JobExecutionStatus.success);
  }

  @Override
  public void stopEventConnector(String message, JobExecutionStatus jobExecutionStatus) {
    stopConnectorBoolean.set(true);
    super.stopEventConnector(message, jobExecutionStatus);
  }

  @Override
  public AutoCloseableIterator<AirbyteMessage> read(final JsonNode config, final ConfiguredAirbyteCatalog catalog, final JsonNode state) {
    int numberOfConsumers = getNumberOfConsumers(config);
    int threadPoolSize = getThreadPoolSize(numberOfConsumers);
    stopConnectorBoolean.set(false);
    ScheduledExecutorService ses = Executors.newScheduledThreadPool(threadPoolSize);

    Map<String, Object> additionalProperties = catalog.getAdditionalProperties();

    ConfiguredAirbyteStream configuredAirbyteStream = catalog.getStreams().get(0);
    ((ObjectNode) config).put(STREAM_NAME,configuredAirbyteStream.getStream().getName());

    String eventSourceType = getEventSourceType(additionalProperties);
    String connectorId = getConnectorId(additionalProperties);

    if (!config.has("group_id"))
    {
      ((ObjectNode) config).put("group_id","bicycle_"+connectorId);
    }

    BicycleConfig bicycleConfig = getBicycleConfig(additionalProperties, systemAuthenticator);
    setBicycleEventProcessorAndPublisher(bicycleConfig);

    eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);
    MetricAsEventsGenerator metricAsEventsGenerator = new KafkaMetricAsEventsGenerator(bicycleConfig, eventSourceInfo, config, bicycleEventPublisher,this);
    AuthInfo authInfo = bicycleConfig.getAuthInfo();
    try {
      ses.scheduleAtFixedRate(metricAsEventsGenerator, 60, 300, TimeUnit.SECONDS);
      eventConnectorJobStatusNotifier.setNumberOfThreadsRunning(new AtomicInteger(numberOfConsumers));
      eventConnectorJobStatusNotifier.setScheduledExecutorService(ses);
      for (int i = 0; i < numberOfConsumers; i++) {
        Map<String, Long> totalRecordsRead = new HashMap<>();
        String consumerThreadId = UUID.randomUUID().toString();
        consumerToTopicPartitionRecordsRead.put(consumerThreadId, totalRecordsRead);
        BicycleConsumer bicycleConsumer = new BicycleConsumer(consumerThreadId, totalRecordsRead, bicycleConfig, config, catalog,eventSourceInfo, eventConnectorJobStatusNotifier,this);
        ses.schedule(bicycleConsumer, 1, TimeUnit.SECONDS);
      }
      eventConnectorJobStatusNotifier.sendStatus(JobExecutionStatus.processing,"Kafka Event Connector started Successfully", connectorId, getTotalRecordsConsumed(),authInfo);
    } catch (Exception exception) {
      this.stopEventConnector("Shutting down the kafka Event Connector due to exception",JobExecutionStatus.failure);
      LOGGER.error("Shutting down the Kafka Event Connector for connector {}", bicycleConfig.getConnectorId() ,exception);
    }
    return null;
  }

  @Override
  protected int getTotalRecordsConsumed() {
    int totalRecordsConsumed = 0;
    Map<String, Map<String, Long>> consumerThreadToTopicPartitionMessagesRead = getTopicPartitionRecordsRead();
    for (Map.Entry<String, Map<String, Long>> consumerThreadEntry :
            consumerThreadToTopicPartitionMessagesRead.entrySet()) {
      for (Map.Entry<String, Long> entry : consumerThreadEntry.getValue().entrySet()) {
        totalRecordsConsumed += entry.getValue();
      }
    }
    return totalRecordsConsumed;
  }

  @Override
  public List<RawEvent> convertRecordsToRawEvents(List<?> records) {
    Iterator<ConsumerRecord<String, JsonNode>> recordsIterator = (Iterator<ConsumerRecord<String, JsonNode>>) records.iterator();
    List<RawEvent> rawEvents = new ArrayList<>();
    while (recordsIterator.hasNext()) {
      ConsumerRecord<String, JsonNode> record = recordsIterator.next();
      JsonRawEvent jsonRawEvent = new JsonRawEvent(record.value().toString());
      rawEvents.add(jsonRawEvent);
    }
    if (rawEvents.size() == 0) {
      return null;
    }
    return rawEvents;
  }

  @Override
  public AutoCloseableIterator<AirbyteMessage> preview(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) {
    ((ObjectNode) config).put("group_id",CommonUtils.getRandomBicycleUUID());
    ((ObjectNode)config).put("auto_offset_reset", "earliest");
    final AirbyteConnectionStatus check = check(config);
    if (check.getStatus().equals(AirbyteConnectionStatus.Status.FAILED)) {
      throw new RuntimeException("Unable establish a connection: " + check.getMessage());
    }
    ConfiguredAirbyteStream configuredAirbyteStream = (ConfiguredAirbyteStream)catalog.getStreams().get(0);
    ((ObjectNode)config).put("stream_name", configuredAirbyteStream.getStream().getName());

    final KafkaSourceConfig kafkaSourceConfig = new KafkaSourceConfig(UUID.randomUUID().toString(),config, null);
    final KafkaConsumer<String, JsonNode> consumer = kafkaSourceConfig.getConsumer(Command.READ);
    final List<ConsumerRecord<String, JsonNode>> recordsList = new ArrayList<>();

    final int retry = config.has("repeated_calls") ? config.get("repeated_calls").intValue() : 0;
    int pollCount = 0;
    int pollingTime = config.has("polling_time") ? config.get("polling_time").intValue() : 5000;
    String groupId = (config.has("group_id") ? config.get("group_id").asText() : null);
    while (true) {
      try {
        final ConsumerRecords<String, JsonNode> consumerRecords = consumer.poll(Duration.of(pollingTime, ChronoUnit.MILLIS));
        if (consumerRecords.count() == 0) {
          pollCount++;
          if (pollCount > retry) {
            LOGGER.info("Failed to fetch any consumer record for group id " + groupId);
            break;
          }
        } else {
          LOGGER.info("Consumer Record count " + consumerRecords.count() + " for group id " + groupId);
          consumerRecords.forEach(record -> {
            LOGGER.info("Consumer Record: key - {}, value - {}, partition - {}, offset - {}",
                    record.key(), record.value(), record.partition(), record.offset());
            recordsList.add(record);
          });
          break;
        }
      } catch (Exception e) {
        LOGGER.error("Exception occurred for group id " + groupId + " while fetching consumer records. Error Message: " + e.getMessage(), e);
        break;
      }
    }

    consumer.close();
    final Iterator<ConsumerRecord<String, JsonNode>> iterator = recordsList.iterator();

    return AutoCloseableIterators.fromIterator(new AbstractIterator<>() {

      @Override
      protected AirbyteMessage computeNext() {
        if (iterator.hasNext()) {
          final ConsumerRecord<String, JsonNode> record = iterator.next();
          return new AirbyteMessage()
                  .withType(AirbyteMessage.Type.RECORD)
                  .withRecord(new AirbyteRecordMessage()
                          .withStream(record.topic())
                          .withEmittedAt(Instant.now().toEpochMilli())
                          .withData(record.value()));
        }

        return endOfData();
      }

    });
  }

  @Override
  public AutoCloseableIterator<AirbyteMessage> syncData(JsonNode sourceConfig,
                                                        ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                                                        JsonNode readState,
                                                        SyncDataRequest syncDataRequest) {

    PropertyValue propVal = syncDataRequest.getTraceInfo().getContextMap().get("enableExistingConnections");
    if (propVal != null && propVal.getBooleanVal() == true) {
      AutoCloseableIterator<AirbyteMessage> nonEmptyIterator =
              super.syncData(sourceConfig, configuredAirbyteCatalog, readState, syncDataRequest);
      if (nonEmptyIterator != null) {
        return null;
      }
    } else {
      LOGGER.info("Received sync data request for {}", syncDataRequest);
    }

    String traceInfo = CommonUtil.getTraceInfo(syncDataRequest.getTraceInfo());
    int numberOfConsumers = getNumberOfConsumers(sourceConfig);
    int threadPoolSize = numberOfConsumers; // since there are no metrics, no additional thread for metric
    ScheduledExecutorService ses = Executors.newScheduledThreadPool(threadPoolSize);

    Map<String, Object> additionalProperties = configuredAirbyteCatalog.getAdditionalProperties();
    String eventSourceType = getEventSourceType(additionalProperties);
    String connectorId = getConnectorId(additionalProperties);
    BicycleConfig bicycleConfig = getBicycleConfig(additionalProperties, systemAuthenticator);
    setBicycleEventProcessorAndPublisher(bicycleConfig);

    ConfiguredAirbyteStream configuredAirbyteStream = configuredAirbyteCatalog.getStreams().get(0);
    sourceConfig = ((ObjectNode) sourceConfig).put(STREAM_NAME,configuredAirbyteStream.getStream().getName());
    if (!sourceConfig.has("group_id"))
    {
      sourceConfig = ((ObjectNode) sourceConfig).put("group_id","bicycle_"+connectorId);
    }

    eventSourceInfo = new EventSourceInfo(connectorId, eventSourceType);

    try {
      for (int i = 0; i < numberOfConsumers; i++) {
        Map<String, Long> totalRecordsRead = new HashMap<>();
        String consumerThreadId = UUID.randomUUID().toString();
        consumerToTopicPartitionRecordsRead.put(consumerThreadId, totalRecordsRead);
        BicycleConsumer bicycleConsumer =
                new BicycleConsumer(
                        consumerThreadId,
                        totalRecordsRead,
                        bicycleConfig,
                        sourceConfig,
                        configuredAirbyteCatalog,
                        eventSourceInfo,
                        eventConnectorJobStatusNotifier,
                        this,
                        true,
                        syncDataRequest
                );
        ses.schedule(bicycleConsumer, 1, TimeUnit.SECONDS);
      }
    } catch (Exception exception) {
      LOGGER.error(traceInfo + " Shutting down the Kafka Event Connector for connector {}",
              bicycleConfig.getConnectorId() , exception);
    }
    return null;
  }

  public Map<String, Map<String, Long>> getTopicPartitionRecordsRead() {
    return consumerToTopicPartitionRecordsRead;
  }

  public static void main(final String[] args) throws Exception {
    final Source source = new KafkaSource(null,null);
    LOGGER.info("Starting source: {}", KafkaSource.class);
    new IntegrationRunner(source).run(args);
    LOGGER.info("Completed source: {}", KafkaSource.class);
  }



  private int getNumberOfConsumers(JsonNode sourceConfig) {
    return sourceConfig.has("bicycle_consumer_threads") ?
            sourceConfig.get("bicycle_consumer_threads").asInt() : CONSUMER_THREADS_DEFAULT_VALUE;
  }

  private int getThreadPoolSize(int numberOfConsumers) {
    return numberOfConsumers + 3;
  }

}
