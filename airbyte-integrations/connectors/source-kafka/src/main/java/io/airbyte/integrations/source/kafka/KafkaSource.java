/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.bicycle.base.integration.BicycleAuthInfo;
import io.airbyte.integrations.bicycle.base.integration.BicycleConfig;
import io.airbyte.protocol.models.*;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.bicycle.server.event.mapping.constants.OTELConstants.TENANT_ID;

public class KafkaSource extends BaseEventConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);
  private static boolean setBicycleEventProcessorFlag=false;
  public static final String TRUST_STORE_LOCATION = "trust_store_location";
  public static final String TRUST_STORE_PASSWORD = "trust_store_password";
  public static final String STREAM_NAME = "stream_name";
  private static final String CONSUMER_THREADS_DEFAULT_VALUE = "2";
  private static final String CONSUMER_THREADS = "INCEPTION_CONSUMER_THREADS";
  private static final Map<String, Map<String, Long>> consumerToTopicPartitionRecordsRead = new HashMap<>();

  public KafkaSource() {
    super();
  }

  @Override
  public AirbyteConnectionStatus check(final JsonNode config) {
    KafkaSourceConfig kafkaSourceConfig = new KafkaSourceConfig(UUID.randomUUID().toString(), config);
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
    KafkaSourceConfig kafkaSourceConfig = new KafkaSourceConfig(UUID.randomUUID().toString(), config);
    final Set<String> topicsToSubscribe = kafkaSourceConfig.getTopicsToSubscribe();
    final List<AirbyteStream> streams = topicsToSubscribe.stream().map(topic -> CatalogHelpers
        .createAirbyteStream(topic, Field.of("value", JsonSchemaType.STRING))
        .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL)))
        .collect(Collectors.toList());
    return new AirbyteCatalog().withStreams(streams);
  }

  @Override
  public AutoCloseableIterator<AirbyteMessage> read(final JsonNode config, final ConfiguredAirbyteCatalog catalog, final JsonNode state) {
    int numberOfConsumers =config.has("consumer_threads") ? config.get("consumer_threads").asInt(): 2;
    int threadPoolSize = numberOfConsumers + 3;
    ScheduledExecutorService ses = Executors.newScheduledThreadPool(threadPoolSize);

    Map<String, Object> additionalProperties = catalog.getAdditionalProperties();

    ConfiguredAirbyteStream configuredAirbyteStream = catalog.getStreams().get(0);
    ((ObjectNode) config).put(STREAM_NAME,configuredAirbyteStream.getStream().getName());

    String serverURL = additionalProperties.containsKey("bicycleServerURL") ? additionalProperties.get("bicycleServerURL").toString() : "";
    String uniqueIdentifier = UUID.randomUUID().toString();
    String token = additionalProperties.containsKey("bicycleToken") ? additionalProperties.get("bicycleToken").toString() : "";
    String connectorId = additionalProperties.containsKey("bicycleConnectorId") ? additionalProperties.get("bicycleConnectorId").toString() : "";
    String eventSourceType= additionalProperties.containsKey("bicycleEventSourceType") ? additionalProperties.get("bicycleEventSourceType").toString() : "";

    BicycleConfig bicycleConfig = new BicycleConfig(serverURL, token, connectorId, uniqueIdentifier);
    if (!setBicycleEventProcessorFlag) {
      setBicycleEventProcessor(bicycleConfig);
    }
    BicycleAuthInfo authInfo = new BicycleAuthInfo(bicycleConfig.getToken(), TENANT_ID);
    EventSourceInfo eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);

    try {
//      ses.scheduleAtFixedRate(metricAsEventsGenerator, 60, 300, TimeUnit.SECONDS);
      for (int i = 0; i < numberOfConsumers; i++) {
        Map<String, Long> totalRecordsRead = new HashMap<>();
        String consumerThreadId = UUID.randomUUID().toString();
        consumerToTopicPartitionRecordsRead.put(consumerThreadId, totalRecordsRead);
        BicycleConsumer bicycleConsumer = new BicycleConsumer(consumerThreadId, totalRecordsRead, bicycleConfig, config, catalog,authInfo,eventSourceInfo,this);
        ses.schedule(bicycleConsumer, 1, TimeUnit.SECONDS);
      }
    } catch (Exception exception) {
      LOGGER.error("Shutting down the kafka consumer application", exception);
      ses.shutdown();
    }
    return null;
  }

  @Override
  protected List<RawEvent> convertRecordsToRawEvents(List<?> records) {
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

  public static void main(final String[] args) throws Exception {
    final Source source = new KafkaSource();
    LOGGER.info("Starting source: {}", KafkaSource.class);
    new IntegrationRunner(source).run(args);
    LOGGER.info("Completed source: {}", KafkaSource.class);
  }

}
