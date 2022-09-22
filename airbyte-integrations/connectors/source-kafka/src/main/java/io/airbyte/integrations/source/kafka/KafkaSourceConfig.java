/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.FileOutputStream;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.base.Command;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.airbyte.integrations.source.kafka.KafkaSource.*;


public class KafkaSourceConfig {

  protected static final Logger LOGGER = LoggerFactory.getLogger(KafkaSourceConfig.class);

  private final JsonNode config;
  private KafkaConsumer<String, JsonNode> consumer;
  private Set<String> topicsToSubscribe;
  private boolean trustStoreFileInitialized;
  private final String consumerThreadName;
  private final String trustStoreFilePath;
  public KafkaSourceConfig(String consumerThreadName, final JsonNode config) {
    this.config = config;
    this.consumerThreadName = consumerThreadName;
    this.trustStoreFilePath = "/tmp/bicycle/kafka/" + UUID.randomUUID().toString() + "/client.truststore.jks";
  }


  private Map<String, Object> getDefaultProps(final JsonNode config) {
    final Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get("bootstrap_servers").asText());
    props.put(ConsumerConfig.GROUP_ID_CONFIG,
            config.has("group_id") ? config.get("group_id").asText() : null);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
            config.has("max_poll_records") ? config.get("max_poll_records").intValue() : null);
    props.putAll(propertiesByProtocol(config));
    props.put(ConsumerConfig.CLIENT_ID_CONFIG,
            config.has("client_id") ? config.get("client_id").asText() : null);
    //   props.put(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG, config.get("client_dns_lookup").asText());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.get("enable_auto_commit").booleanValue());
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
            config.has("auto_commit_interval_ms") ? config.get("auto_commit_interval_ms").intValue() : null);
    props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG,
            config.has("retry_backoff_ms") ? config.get("retry_backoff_ms").intValue() : null);
    props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
            config.has("request_timeout_ms") ? config.get("request_timeout_ms").intValue() : null);
    props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG,
            config.has("receive_buffer_bytes") ? config.get("receive_buffer_bytes").intValue() : null);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            config.has("auto_offset_reset") ? config.get("auto_offset_reset").asText() : null);

    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,
            config.has("fetch_min_bytes") ? config.get("fetch_min_bytes").intValue() : 1024);

    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
            config.has("fetch_max_wait_ms") ? config.get("fetch_max_wait_ms").intValue() : 500);

    props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
            config.has("max_partition_fetch_bytes") ? config.get("max_partition_fetch_bytes").intValue()
                    : 1 * 1024 * 1024);

    return props;
  }

  private KafkaConsumer<String, String> buildMetricsConsumer(final JsonNode config) {

    Map<String, Object> props = getDefaultProps(config);

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "metrics-consumer");
    final Map<String, Object> filteredProps = props.entrySet().stream()
            .filter(entry -> entry.getValue() != null && !entry.getValue().toString().isBlank())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    filteredProps.put("ssl.endpoint.identification.algorithm", "");

    return new KafkaConsumer<>(filteredProps);
  }

  private KafkaConsumer<String, JsonNode> buildKafkaConsumer(final JsonNode config) {

    Map<String, Object> props = getDefaultProps(config);

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());

    final Map<String, Object> filteredProps = props.entrySet().stream()
            .filter(entry -> entry.getValue() != null && !entry.getValue().toString().isBlank())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    filteredProps.put("ssl.endpoint.identification.algorithm", "");

    return new KafkaConsumer<>(filteredProps);
  }

  private Map<String, Object> propertiesByProtocol(final JsonNode config) {
    final JsonNode protocolConfig = config.get("protocol");
    LOGGER.debug("Kafka protocol config: {}", protocolConfig.toString());
    final KafkaProtocol protocol =
            KafkaProtocol.valueOf(protocolConfig.get("security_protocol").asText().toUpperCase());
    final ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol.toString());

    switch (protocol) {
      case PLAINTEXT:
        break;
      case SASL_SSL:
      case SASL_PLAINTEXT:
        builder.put(SaslConfigs.SASL_JAAS_CONFIG, protocolConfig.get("sasl_jaas_config").asText());
        builder.put(SaslConfigs.SASL_MECHANISM, protocolConfig.get("sasl_mechanism").asText());
        addTruststoreRelatedConfig(builder, protocolConfig);
        break;
      default:
        throw new RuntimeException("Unexpected Kafka protocol: " + Jsons.serialize(protocol));
    }

    return builder.build();
  }

  private void addTruststoreRelatedConfig(ImmutableMap.Builder<String, Object> builder, JsonNode protocolConfig) {

    String path = getTrustStoreFilePath(protocolConfig);

    if (path != null) {
      if (!trustStoreFileInitialized) {
        LOGGER.info("Truststore path is" + path);
      }
      trustStoreFileInitialized = true;
      builder.put("ssl.truststore.type", "jks");
      builder.put("ssl.endpoint.identification.algorithm", "");
      builder.put("ssl.truststore.location", path);
      String trustStorePass = protocolConfig.has("ssl_truststore_password") ?
              protocolConfig.get("ssl_truststore_password").asText() : null;
      if (trustStorePass != null) {
        builder.put("ssl.truststore.password", trustStorePass);
      } else {
        throw new RuntimeException("Truststore password is missing");
      }

    }
  }

  private String getTrustStoreFilePath(JsonNode config) {

    try {
      String trustStoreEncodedContent = config.has("ssl_truststore_certificate_content") ?
              config.get("ssl_truststore_certificate_content").asText() : null;
      if (StringUtils.isEmpty(trustStoreEncodedContent)) {
        return null;
      }
      if (trustStoreFileInitialized) {
        return this.trustStoreFilePath;
      }
      File file = new File(this.trustStoreFilePath);
      if (file.exists()) {
        return this.trustStoreFilePath;
      }

      if (trustStoreEncodedContent != null) {
        byte[] decodedBytes = Base64.getDecoder().decode(trustStoreEncodedContent);
        writeToFile(decodedBytes);
        return this.trustStoreFilePath;
      }
    } catch (Exception e) {
      LOGGER.error("Unable to get truststore file path", e);
    }

    return null;
  }

  public void writeToFile(byte[] content) {
    try {
      File outputFile =
              new File(this.trustStoreFilePath);
      outputFile.getParentFile().mkdirs();
      try (FileOutputStream outputStream = new FileOutputStream(outputFile)) {
        outputStream.write(content);
      }
    } catch (Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  public KafkaConsumer<String, JsonNode> getConsumer(Command command) {
    if (consumer != null) {
      return consumer;
    }

    consumer = buildKafkaConsumer(config);
    LOGGER.info("Kafka consumer running with config {} ", config);

    final JsonNode subscription = config.get("subscription");
    LOGGER.info("Kafka subscribe method: {}", subscription.toString());
    switch (subscription.get("subscription_type").asText()) {
      case "subscribe":
        if (Command.READ.equals(command)) {
          topicsToSubscribe = new HashSet<>();
          String topic = config.get(STREAM_NAME).asText();
          consumer.subscribe(Collections.singletonList(topic), new HandleRebalance());
          topicsToSubscribe.add(topic);
        } else {
          final String topicPattern = subscription.get("topic_pattern").asText();
          consumer.subscribe(Pattern.compile(topicPattern));
          topicsToSubscribe = consumer.listTopics().keySet().stream()
                  .filter(topic -> topic.matches(topicPattern))
                  .collect(Collectors.toSet());
        }
        break;
      case "assign":
        topicsToSubscribe = new HashSet<>();
        final String topicPartitions = subscription.get("topic_partitions").asText();
        final String[] topicPartitionsStr = topicPartitions.replaceAll("\\s+", "").split(",");
        final List<TopicPartition> topicPartitionList =
                Arrays.stream(topicPartitionsStr).map(topicPartition -> {
                  final String[] pair = topicPartition.split(":");
                  topicsToSubscribe.add(pair[0]);
                  return new TopicPartition(pair[0], Integer.parseInt(pair[1]));
                }).collect(Collectors.toList());
        LOGGER.info("Topic-partition list: {}", topicPartitionList);
        consumer.assign(topicPartitionList);
        break;
      default:
        break;
    }

    return consumer;
  }

  public KafkaConsumer<String, JsonNode> getCheckConsumer() {
    return buildKafkaConsumer(config);
  }

  public KafkaConsumer<String, String> getMetricsConsumer() {
    return buildMetricsConsumer(config);
  }

  public Properties getAdminProperties() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get("bootstrap_servers").asText());
    properties.putAll(propertiesByProtocol(config));
    return properties;
  }

  public void resetConsumer() {
    consumer = null;
  }

  public Set<String> getTopicsToSubscribe() {
    return topicsToSubscribe;
  }

  private class HandleRebalance implements ConsumerRebalanceListener {

    public void onPartitionsAssigned(Collection<TopicPartition>
                                             partitions) {
      LOGGER.info("Partitions assigned for consumer {} are {}", consumerThreadName, partitions);
    }

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      LOGGER.info("Partitions revoked for consumer {} are {}", consumerThreadName, partitions);
    }
  }

/*    public static void publishJMXMetric() {



        String host = "localhost";  // or some A.B.C.D
        int port = 1234;
        String url = "service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi";
        JMXConnector jmxConnector = null;
        try {

            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            Set<ObjectName> objectNames = server.queryNames(null, null);

            LOGGER.info("========bean set start======");
            LOGGER.info(objectNames.toString());
            LOGGER.info("========bean set end======");



        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            try {
                jmxConnector.close();
            } catch (IOException ioException) {
             //   ioException.printStackTrace();
            }
        }

    }*/

}
