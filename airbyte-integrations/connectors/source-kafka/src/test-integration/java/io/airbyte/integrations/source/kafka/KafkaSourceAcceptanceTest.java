/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.inception.server.auth.api.SystemAuthenticator;
import io.airbyte.commons.jackson.MoreMappers;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.integrations.bicycle.base.integration.BicycleConfig;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.integrations.standardtest.source.EventSourceAcceptanceTest;
import io.airbyte.integrations.standardtest.source.TestDestinationEnv;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.SyncMode;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class KafkaSourceAcceptanceTest extends EventSourceAcceptanceTest {

  private static final ObjectMapper mapper = MoreMappers.initMapper();
  private static final String TOPIC_NAME = "test.topic";

  private static KafkaContainer KAFKA;

  private static BicycleConsumer bicycleConsumer;
  private static JsonNode config;
  private static ConfiguredAirbyteCatalog catalog;

  @BeforeAll
  public static void setupBicycleConsumer() {
    String serverURL =  "";
    String uniqueIdentifier = UUID.randomUUID().toString();
    String token = "";
    String connectorId = "";
    String eventSourceType= "EVENT";
    String tenantId = "";

    Map<String, Long> totalRecordsRead = null;
    BicycleConfig bicycleConfig = new BicycleConfig(serverURL, token, connectorId, uniqueIdentifier, tenantId, Mockito.mock(SystemAuthenticator.class), true);
    EventSourceInfo eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);

    config = null;
    catalog= new ConfiguredAirbyteCatalog();

    String consumerThreadId = UUID.randomUUID().toString();

    bicycleConsumer = new BicycleConsumer(consumerThreadId, totalRecordsRead, bicycleConfig, config, catalog, eventSourceInfo, Mockito.mock(EventConnectorJobStatusNotifier.class), Mockito.mock(KafkaSource.class));
  }

  @Override
  protected String getImageName() {
    return "airbyte/source-kafka:dev";
  }

  @Override
  protected JsonNode getConfig() {
    final ObjectNode protocolConfig = mapper.createObjectNode();
    final ObjectNode subscriptionConfig = mapper.createObjectNode();
    protocolConfig.put("security_protocol", KafkaProtocol.PLAINTEXT.toString());
    subscriptionConfig.put("subscription_type", "subscribe");
    subscriptionConfig.put("topic_pattern", TOPIC_NAME);

    return Jsons.jsonNode(ImmutableMap.builder()
        .put("bootstrap_servers", KAFKA.getBootstrapServers())
        .put("subscription", subscriptionConfig)
        .put("client_dns_lookup", "use_all_dns_ips")
        .put("enable_auto_commit", false)
        .put("group_id", "groupid")
        .put("repeated_calls", 3)
        .put("protocol", protocolConfig)
        .put("auto_offset_reset", "earliest")
        .build());
  }

  @Override
  protected void setupEnvironment(final TestDestinationEnv environment) throws Exception {
    KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.0"));
    KAFKA.start();

    createTopic();
    sendEvent();
  }

  private void sendEvent() throws ExecutionException, InterruptedException {
    final Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers())
        .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
        .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName())
        .build();
    final KafkaProducer<String, JsonNode> producer = new KafkaProducer<>(props);

    final ObjectNode event = mapper.createObjectNode();
    event.put("test", "value");

    producer.send(new ProducerRecord<>(TOPIC_NAME, event), (recordMetadata, exception) -> {
      if (exception != null) {
        throw new RuntimeException("Cannot send message to Kafka. Error: " + exception.getMessage(), exception);
      }
    }).get();
  }

  private void createTopic() throws Exception {
    try (final var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
      final NewTopic topic = new NewTopic(TOPIC_NAME, 1, (short) 1);
      admin.createTopics(Collections.singletonList(topic)).all().get();
    }
  }

  @Override
  protected void tearDown(final TestDestinationEnv testEnv) {
    KAFKA.close();
  }

  @Override
  protected ConnectorSpecification getSpec() throws Exception {
    return Jsons.deserialize(MoreResources.readResource("spec.json"), ConnectorSpecification.class);
  }

  @Override
  protected ConfiguredAirbyteCatalog getConfiguredCatalog() throws Exception {
    final ConfiguredAirbyteStream streams =
        CatalogHelpers.createConfiguredAirbyteStream(TOPIC_NAME, null, Field.of("value", JsonSchemaType.STRING));
    streams.setSyncMode(SyncMode.FULL_REFRESH);
    return new ConfiguredAirbyteCatalog().withStreams(Collections.singletonList(streams));
  }

  @Override
  protected JsonNode getState() throws Exception {
    return Jsons.jsonNode(new HashMap<>());
  }

  // need to shift to unit-test?
  @Test
  protected void testBicycleConsumerCheck() {
    config=getConfig();
    ((ObjectNode) config).put("test_topic",TOPIC_NAME);
    Assertions.assertEquals(bicycleConsumer.check(config),true);
  }


}
