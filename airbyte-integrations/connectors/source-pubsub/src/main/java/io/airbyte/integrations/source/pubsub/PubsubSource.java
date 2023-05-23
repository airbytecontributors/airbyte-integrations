package io.airbyte.integrations.source.pubsub;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.pubsub.v1.*;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.bicycle.base.integration.*;
import io.airbyte.protocol.models.*;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class PubsubSource extends BaseEventConnector {
    private AtomicLong totalBytesProcessed = new AtomicLong(0);
    private AtomicBoolean stopConnectorBoolean = new AtomicBoolean(false);
    private final String PARTITION_TIMESTAMP = "recordTimestamp";
    private static final int CONSUMER_THREADS_DEFAULT_VALUE = 1;
    public static final String STREAM_NAME = "stream_name";
    public static final String TEST_SOURCE_CONFIG = "test_source_config";
    private static final Logger LOGGER = LoggerFactory.getLogger(PubsubSource.class);
    private Map<String, Long> consumerToSubscriptionRecordsRead = new HashMap<>();

    public PubsubSourceConfig getPubsubSourceConfig() {
        return pubsubSourceConfig;
    }

    protected PubsubSourceConfig pubsubSourceConfig;
    public PubsubSource(SystemAuthenticator systemAuthenticator, EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier,
                        ConnectorConfigManager connectorConfigManager) {
        super(systemAuthenticator, eventConnectorJobStatusNotifier, connectorConfigManager);
    }

    protected AtomicBoolean getStopConnectorBoolean() {
        return stopConnectorBoolean;
    }


    @Override
    protected int getTotalRecordsConsumed() {
        int totalRecordsConsumed = 0;
        Map<String,Long> consumerToSubscriptionRecordsRead = getConsumerToSubscriptionRecordsRead();
        for (Map.Entry<String, Long> consumerThreadEntry :
                consumerToSubscriptionRecordsRead.entrySet()) {
            totalRecordsConsumed += consumerThreadEntry.getValue();
        }
        return totalRecordsConsumed;
    }

    public void stopEventConnector() {
        stopConnectorBoolean.set(true);
        super.stopEventConnector("Pubsub Event Connector Stopped manually", JobExecutionStatus.success);
    }

    @Override
    public void stopEventConnector(String message, JobExecutionStatus jobExecutionStatus) {
        stopConnectorBoolean.set(true);
        super.stopEventConnector(message, jobExecutionStatus);
    }

    @Override
    public List<RawEvent> convertRecordsToRawEvents(List<?> records) {
        Iterator<ReceivedMessage> recordsIterator = (Iterator<ReceivedMessage>) records.iterator();
        List<RawEvent> rawEvents = new ArrayList<>();
        boolean printed = false;
        ObjectMapper objectMapper = CommonUtils.getObjectMapper();
        while (recordsIterator.hasNext()) {
            ReceivedMessage record = recordsIterator.next();
            JsonNode jsonNode = null;
            try {
                PubsubMessage pubsubMessage = record.getMessage();
                jsonNode = objectMapper.readTree(pubsubMessage.getData().toString(Charset.defaultCharset()));
                if (jsonNode.isTextual()) {
                    jsonNode = objectMapper.readTree(jsonNode.textValue());
                }
                Map<String, String> customAttributesMap = pubsubMessage.getAttributesMap();
                ObjectNode objectNode = (ObjectNode) jsonNode;
                for (Map.Entry<String, String> customAttributeEntry : customAttributesMap.entrySet()) {
                    objectNode.put(customAttributeEntry.getKey(), customAttributeEntry.getValue());
                }
                objectNode.put(PARTITION_TIMESTAMP, pubsubMessage.getPublishTime().getNanos());
            } catch (Exception e) {
                if (!printed) {
                    LOGGER.error("Error while adding record metadata {}", e);
                    printed = true;
                }
            }

            JsonRawEvent jsonRawEvent = new JsonRawEvent(jsonNode);
            rawEvents.add(jsonRawEvent);
        }
        return rawEvents;
    }

    protected AtomicLong getTotalBytesProcessed() {
        return totalBytesProcessed;
    }

    @Override
    public AutoCloseableIterator<AirbyteMessage> preview(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) throws InterruptedException, ExecutionException {
        final AirbyteConnectionStatus check = check(config);
        if (check.getStatus().equals(AirbyteConnectionStatus.Status.FAILED)) {
            throw new RuntimeException("Unable establish a connection: " + check.getMessage());
        }
        ConfiguredAirbyteStream configuredAirbyteStream = catalog.getStreams().get(0);
        ((ObjectNode) config).put(STREAM_NAME,configuredAirbyteStream.getStream().getName());

        String streamName = configuredAirbyteStream.getStream().getName();
        PubsubSourceConfig pubsubSourceConfig = new PubsubSourceConfig(UUID.randomUUID().toString(), config, "preview");
        final SubscriptionAdminClient consumer = pubsubSourceConfig.getConsumer();
        List<ReceivedMessage> recordsList;
        String subscriptionId = pubsubSourceConfig.getOrCreateSubscriptionId();
        PullRequest pullRequest = pubsubSourceConfig.getPullRequest(subscriptionId);
        PullResponse pullResponse =
                consumer.pull(pullRequest);
        if (pubsubSourceConfig.isBicycleSubscription()) {
            pubsubSourceConfig.deleteSubscription(subscriptionId);
        }
        consumer.close();
        recordsList = pullResponse.getReceivedMessagesList();
        final Iterator<ReceivedMessage> iterator = recordsList.iterator();

        return AutoCloseableIterators.fromIterator(new AbstractIterator<>() {
            @Override
            protected AirbyteMessage computeNext() {
                if (iterator.hasNext()) {
                    final ReceivedMessage record = iterator.next();
                    JsonNode previewMessage = null;
                    try {
                        previewMessage = CommonUtils.getObjectMapper().readTree(record.getMessage().getData().toStringUtf8());
                    } catch (JsonProcessingException e) {
                    }
                    return new AirbyteMessage()
                            .withType(AirbyteMessage.Type.RECORD)
                            .withRecord(new AirbyteRecordMessage()
                                    .withStream(streamName)
                                    .withEmittedAt(Instant.now().toEpochMilli())
                                    .withData(previewMessage));
                }

                return endOfData();
            }

        });
    }

    @Override
    public AirbyteConnectionStatus check(JsonNode config) {
        PubsubSourceConfig pubsubSourceConfig = new PubsubSourceConfig(TEST_SOURCE_CONFIG, config, null);
        SubscriptionAdminClient checkConsumer = null;
        try {
            final String subscriptionId = config.has("subscription_id") ? config.get("subscription_id").asText() : "";
            if (!subscriptionId.isBlank()) {
                checkConsumer = pubsubSourceConfig.getCheckConsumer();
                PullResponse pullResponse = checkConsumer.pull(pubsubSourceConfig.getCheckPullRequest(subscriptionId));
                LOGGER.info("Successfully pulled messages {}", pullResponse);
            }
            return new AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.SUCCEEDED);
        } catch (final Exception e) {
            LOGGER.error("Exception attempting to connect to the Pubsub Subscription: ", e);
            return new AirbyteConnectionStatus()
                    .withStatus(AirbyteConnectionStatus.Status.FAILED)
                    .withMessage("Could not connect to the Kafka brokers with provided configuration. \n" + e.getMessage());
        } finally {
            if (checkConsumer != null) {
                checkConsumer.close();
            }
        }
    }

    @Override
    public AirbyteCatalog discover(JsonNode config) throws Exception {
        PubsubSourceConfig pubsubSourceConfig = new PubsubSourceConfig(TEST_SOURCE_CONFIG, config, "");
        final Set<String> subscriptions = new HashSet<>();
        ProjectName projectName = pubsubSourceConfig.getProjectName();
        TopicAdminClient.ListTopicsPagedResponse listTopicsPagedResponse = null;
        String subscriptionId = config.has("subscription_id") ? config.get("subscription_id").asText() : "";
        if (subscriptionId != "") {
            try {
                SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(SubscriptionAdminSettings.newBuilder().setCredentialsProvider(pubsubSourceConfig.getGCPCredentials()).build());
                Subscription subscription = subscriptionAdminClient.getSubscription(pubsubSourceConfig.getProjectSubscriptionName(subscriptionId).toString());
                String topic = subscription.getTopic();
                subscriptionAdminClient.close();
                String[] strings = topic.split("/");
                subscriptions.add(strings[strings.length-1]);
            } catch (Exception e) {
                LOGGER.error("Unable to get subscription details because", e);
            }
        } else {
            try {
                TopicAdminClient topicAdminClient = TopicAdminClient.create(TopicAdminSettings.newBuilder().setCredentialsProvider(pubsubSourceConfig.getGCPCredentials()).build());
                listTopicsPagedResponse = topicAdminClient.listTopics(projectName);
                for (Topic topic : listTopicsPagedResponse.iterateAll()) {
                    String[] topicNameSeparated = topic.getName().split("/");
                    subscriptions.add(topicNameSeparated[topicNameSeparated.length - 1]);
                }
                topicAdminClient.close();
            } catch (Exception e) {
                LOGGER.error("Unable to list topics because", e);
            }
        }
        final List<AirbyteStream> streams = subscriptions.stream().map(stream -> CatalogHelpers
                        .createAirbyteStream(stream, Field.of("value", JsonSchemaType.STRING))
                        .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL)))
                .collect(Collectors.toList());
        return new AirbyteCatalog().withStreams(streams);
    }

    private int getNumberOfConsumers(JsonNode sourceConfig) {
        return sourceConfig.has("bicycle_consumer_threads") ?
                sourceConfig.get("bicycle_consumer_threads").asInt() : CONSUMER_THREADS_DEFAULT_VALUE;
    }

    private int getThreadPoolSize(int numberOfConsumers) {
        return numberOfConsumers + 3;
    }


    @Override
    public AutoCloseableIterator<AirbyteMessage> read(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) throws Exception {

        int numberOfConsumers = getNumberOfConsumers(config);
        int threadPoolSize = getThreadPoolSize(numberOfConsumers);
        stopConnectorBoolean.set(false);
        ScheduledExecutorService ses = Executors.newScheduledThreadPool(threadPoolSize);

        Map<String, Object> additionalProperties = catalog.getAdditionalProperties();

        ConfiguredAirbyteStream configuredAirbyteStream = catalog.getStreams().get(0);
        ((ObjectNode) config).put(STREAM_NAME,configuredAirbyteStream.getStream().getName());

        String eventSourceType = getEventSourceType(additionalProperties);
        String connectorId = getConnectorId(additionalProperties);

        BicycleConfig bicycleConfig = getBicycleConfig(additionalProperties, systemAuthenticator);
        setBicycleEventProcessorAndPublisher(bicycleConfig);

        eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);
        pubsubSourceConfig = new PubsubSourceConfig("PubsubSource", config, bicycleConfig.getConnectorId());
        String subscriptionId = pubsubSourceConfig.getOrCreateSubscriptionId();
        MetricAsEventsGenerator metricAsEventsGenerator = new PubsubMetricAsEventsGenerator(bicycleConfig, eventSourceInfo, config, bicycleEventPublisher,this);
        try {
            ses.scheduleAtFixedRate(metricAsEventsGenerator, 60, 60, TimeUnit.SECONDS);
            eventConnectorJobStatusNotifier.setNumberOfThreadsRunning(new AtomicInteger(numberOfConsumers));
            eventConnectorJobStatusNotifier.setScheduledExecutorService(ses);
            for (int i = 0; i < numberOfConsumers; i++) {
                String consumerThreadId = UUID.randomUUID().toString();
                BicycleConsumer bicycleConsumer = new BicycleConsumer(consumerThreadId, bicycleConfig, config, catalog, eventSourceInfo, eventConnectorJobStatusNotifier,this);
                ses.schedule(bicycleConsumer, 1, TimeUnit.SECONDS);
            }
            AuthInfo authInfo = bicycleConfig.getAuthInfo();
            eventConnectorJobStatusNotifier.sendStatus(JobExecutionStatus.processing,"Pubsub Event Connector started Successfully", connectorId, getTotalRecordsConsumed(),authInfo);
        } catch (Exception exception) {
            this.stopEventConnector("Shutting down the Pubsub Event Connector due to exception",JobExecutionStatus.failure);
            LOGGER.error("Shutting down the Pubsub Event Connector for connector {}", bicycleConfig.getConnectorId() ,exception);
        }
        return null;

    }

    public Map<String, Long> getConsumerToSubscriptionRecordsRead() {
        return consumerToSubscriptionRecordsRead;
    }
}
