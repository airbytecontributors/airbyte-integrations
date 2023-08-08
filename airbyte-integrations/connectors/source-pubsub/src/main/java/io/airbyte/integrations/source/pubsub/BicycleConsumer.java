package io.airbyte.integrations.source.pubsub;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.integration.common.writer.Writer;
import io.bicycle.integration.connector.SyncDataRequest;
import io.bicycle.server.event.mapping.UserServiceMappingRule;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/**
 */
public class BicycleConsumer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(BicycleConsumer.class.getName());
    private final PubsubSourceConfig pubsubSourceConfig;
    private final JsonNode config;
    private final BicycleConfig bicycleConfig;
    private final String name;
    private final ConfiguredAirbyteCatalog catalog;
    private EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier;
    private final PubsubSource pubsubSource;
    private final EventSourceInfo eventSourceInfo;
    private final boolean isDestinationSyncConnector;
    private final SyncDataRequest syncDataRequest;

    public BicycleConsumer(String name, BicycleConfig bicycleConfig, JsonNode connectorConfig, ConfiguredAirbyteCatalog configuredCatalog, EventSourceInfo eventSourceInfo, EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier, PubsubSource instance) {
        this(name, bicycleConfig, connectorConfig, configuredCatalog, eventSourceInfo,
                eventConnectorJobStatusNotifier, instance, false, null);
    }

    public BicycleConsumer(String name,
                           BicycleConfig bicycleConfig,
                           JsonNode connectorConfig,
                           ConfiguredAirbyteCatalog configuredCatalog,
                           EventSourceInfo eventSourceInfo,
                           EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier,
                           PubsubSource instance,
                           boolean isDestinationSyncConnector,
                           SyncDataRequest syncDataRequest) {
        this.name = name;
        this.config = connectorConfig;
        this.catalog = configuredCatalog;
        this.pubsubSourceConfig = new PubsubSourceConfig(name, config, getConnectorId(catalog));
        this.bicycleConfig = bicycleConfig;
        this.eventConnectorJobStatusNotifier = eventConnectorJobStatusNotifier;
        this.pubsubSource = instance;
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
                read(bicycleConfig, config, catalog, null);
            } catch (Exception exception) {
                if (pubsubSourceConfig.getConsumer()!= null) {
                    pubsubSourceConfig.getConsumer().close();
                }
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
            eventConnectorJobStatusNotifier.sendStatus(JobExecutionStatus.failure,"Shutting down the kafka Event Connector", eventSourceInfo.getEventSourceId(), this.pubsubSource.getTotalRecordsConsumed(), authInfo);
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

    public void read(BicycleConfig bicycleConfig, final JsonNode config, final ConfiguredAirbyteCatalog configuredAirbyteCatalog, final JsonNode state) {
        String subscriptionId = pubsubSourceConfig.getOrCreateSubscriptionId();
        final boolean check = check(subscriptionId);

        logger.info("======Starting read operation for consumer " + name + " config: " + config + " catalog:"+ configuredAirbyteCatalog + "=======");
        if (!check) {
            throw new RuntimeException("Unable establish a connection");
        }

        final SubscriptionAdminClient consumer = pubsubSourceConfig.getConsumer();

        int samplingRate = config.has("sampling_rate") ? config.get("sampling_rate").asInt(): 100;

        int sampledRecords = 0;
        while (!this.pubsubSource.getStopConnectorBoolean().get()) {
            List<ReceivedMessage> recordsList;
            PullRequest pullRequest = pubsubSourceConfig.getPullRequest(subscriptionId);
            PullResponse pullResponse =
                    consumer.pull(pullRequest);
            Instant pullResponseTime = Instant.now();
            int counter = 0;
            logger.debug("No of records actually read by consumer {} are {}", name, pullResponse.getReceivedMessagesCount());
            sampledRecords = getNumberOfRecordsToBeReturnedBasedOnSamplingRate(pullResponse.getReceivedMessagesCount(), samplingRate);

            List<String> messageAcks = new ArrayList<>();
            Double totalSize = Double.valueOf(0);
            for (ReceivedMessage record : pullResponse.getReceivedMessagesList()) {
                logger.debug("Consumer Record: key - {}, value - {}",
                        record.getMessage().getMessageId(), record.getMessage().getData().toStringUtf8());

                if (counter > sampledRecords) {
                    break;
                }
                totalSize += record.getMessage().getSerializedSize();
                messageAcks.add(record.getAckId());
                counter++;
            }
            recordsList = pullResponse.getReceivedMessagesList();
            long finalByteTotalSize = totalSize.longValue();
            this.pubsubSource.getTotalBytesProcessed().getAndUpdate(n->n+ finalByteTotalSize);
            Long currentConsumerRecords = this.pubsubSource.getConsumerToSubscriptionRecordsRead().get(name);
            if (currentConsumerRecords == null) {
                currentConsumerRecords = Long.valueOf(0);
            }
            this.pubsubSource.getConsumerToSubscriptionRecordsRead().put(name, currentConsumerRecords + counter);

            logger.info("No of records read from consumer after sampling {} are {} ", name,
                    counter);

            if (recordsList.size() == 0) {
                continue;
            }

            EventProcessorResult eventProcessorResult = null;
            AuthInfo authInfo = bicycleConfig.getAuthInfo();
            try {
                List<RawEvent> rawEvents = this.pubsubSource.convertRecordsToRawEvents(recordsList);
                List<UserServiceMappingRule> userServiceMappingRules =
                        this.pubsubSource.getUserServiceMappingRules(authInfo, eventSourceInfo);
                if (userServiceMappingRules == null) {
                    return;
                }
                eventProcessorResult = this.pubsubSource.convertRawEventsToBicycleEvents(authInfo,eventSourceInfo,
                        rawEvents, userServiceMappingRules);
            } catch (Exception exception) {
                logger.error("Unable to convert raw records to bicycle events for {} ",name, exception);
            }

            try {
                this.pubsubSource.publishEvents(authInfo, eventSourceInfo, eventProcessorResult);
                Instant publishedEventsTime = Instant.now();
                Long timeBetweenPullAndPublish = publishedEventsTime.getEpochSecond() - pullResponseTime.getEpochSecond();
//                if (timeBetweenPullAndPublish > 8) {
//                    consumer.modifyAckDeadline(pubsubSourceConfig.getProjectSubscriptionName(subscriptionId).toString(),
//                            messageAcks, timeBetweenPullAndPublish.intValue() + 5);
//                }
                consumer.acknowledge(pubsubSourceConfig.getProjectSubscriptionName(subscriptionId).toString(), messageAcks);
            } catch (Exception exception) {
                logger.error("Unable to publish bicycle events for {} ", name, exception);
            }
        }
    }

    public void syncData(BicycleConfig bicycleConfig,
                         final JsonNode config,
                         final ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                         final JsonNode state,
                         final SyncDataRequest syncDataRequest,
                         final Writer writer) {

    }

    public boolean check(String subscriptionId) {
        SubscriptionAdminClient checkConsumer = null;
        try {
            if (!subscriptionId.isBlank()) {
                checkConsumer = pubsubSourceConfig.getCheckConsumer();
                PullResponse pullResponse = checkConsumer.pull(pubsubSourceConfig.getCheckPullRequest(subscriptionId));
            }
            return true;
        } catch (final Exception e) {
            logger.error("Exception attempting to connect to the Pubsub Subscription: ", e);
            return false;
        } finally {
            if (checkConsumer != null) {
                checkConsumer.close();
            }
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
