package io.airbyte.integrations.source.pubsub;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.integrations.bicycle.base.integration.BicycleConfig;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.integration.common.writer.Writer;
import io.bicycle.integration.connector.SyncDataRequest;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        final boolean check = check(config);

        logger.info("======Starting read operation for consumer " + name + " config: " + config + " catalog:"+ configuredAirbyteCatalog + "=======");
        if (!check) {
            throw new RuntimeException("Unable establish a connection");
        }

        final SubscriptionAdminClient consumer = pubsubSourceConfig.getConsumer();

        int samplingRate = config.has("sampling_rate") ? config.get("sampling_rate").asInt(): 100;

        int sampledRecords = 0;
        try {
            while (!this.pubsubSource.getStopConnectorBoolean().get()) {
                final List<ReceivedMessage> recordsList = new ArrayList<>();
                PullRequest pullRequest = pubsubSourceConfig.getPullRequest();
                PullResponse pullResponse =
                        consumer.pull(pullRequest);

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
                    recordsList.add(record);
                }
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
                    eventProcessorResult = this.pubsubSource.convertRawEventsToBicycleEvents(authInfo,eventSourceInfo,rawEvents);
                } catch (Exception exception) {
                    logger.error("Unable to convert raw records to bicycle events for {} ",name, exception);
                }

                try {
                    this.pubsubSource.publishEvents(authInfo, eventSourceInfo, eventProcessorResult);
                    consumer.acknowledge(pubsubSourceConfig.getProjectSubscriptionName().toString(), messageAcks);
                } catch (Exception exception) {
                    logger.error("Unable to publish bicycle events for {} ", name, exception);
                }
            }

        } catch (Exception e) {
            logger.error("Consumer stopped because ", e);
        } finally {
            consumer.close();
        }
    }

    public void syncData(BicycleConfig bicycleConfig,
                         final JsonNode config,
                         final ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                         final JsonNode state,
                         final SyncDataRequest syncDataRequest,
                         final Writer writer) {

    }

    public boolean check(final JsonNode config) {
        SubscriberStub consumer = null;
        try {
            final String subscriptionId = config.has("subscription_id") ? config.get("subscription_id").asText() : "";
            if (!subscriptionId.isBlank()) {
                SubscriptionAdminClient checkConsumer = pubsubSourceConfig.getCheckConsumer();
                PullResponse pullResponse = checkConsumer.pull(pubsubSourceConfig.getCheckPullRequest());
                logger.info("Successfully pulled messages {}", pullResponse);
            }
            return true;
        } catch (final Exception e) {
            logger.error("Exception attempting to connect to the Pubsub Subscription: ", e);
            return false;
        } finally {
            if (consumer != null) {
                consumer.close();
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
