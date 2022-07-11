package io.airbyte.integrations.source.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.inception.server.auth.api.SystemAuthenticator;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.BicycleConfig;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorStatusInitiator;
import io.airbyte.protocol.models.*;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.coordinator.Scheduler;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;


public class KinesisSource extends BaseEventConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisSource.class);
    private static final String TENANT_ID = "tenantId";
    public KinesisSource(SystemAuthenticator systemAuthenticator, EventConnectorStatusInitiator eventConnectorStatusHandler) {
        super(systemAuthenticator, eventConnectorStatusHandler);
    }

    @Override
    public AirbyteConnectionStatus check(final JsonNode config) {
        try {
            final String testStream = config.has("test_stream") ? config.get("test_stream").asText() : "";
            if (!testStream.isBlank()) {
                KinesisClientConfig kinesisClientConfig = new KinesisClientConfig(config, this);
                AmazonKinesis kinesis = kinesisClientConfig.getCheckClient();
                StreamDescription streamDesc = kinesis.describeStream(testStream).getStreamDescription();
                List<Shard> shards = streamDesc.getShards();
                LOGGER.info("Successfully connected to Kinesis streams for stream '{}'.",
                        config.get("test_stream").asText());
            }
            return new AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.SUCCEEDED);
        }
        catch (Exception e) {
            LOGGER.error("Exception attempting to connect to the Kinesis streams: ", e);
            return new AirbyteConnectionStatus()
                    .withStatus(AirbyteConnectionStatus.Status.FAILED)
                    .withMessage("Could not connect to the Kinesis streams with provided configuration. \n" + e.getMessage());
        }
    }

    @Override
    public AirbyteCatalog discover(final JsonNode config) throws Exception {
        KinesisClientConfig kinesisClientConfig = new KinesisClientConfig(config, this);
        String streamPattern = kinesisClientConfig.getStreamPattern();
        final Set<String> streamsToSubscribe = kinesisClientConfig.getStreamsToSubscribe(streamPattern);
        final List<AirbyteStream> streams = streamsToSubscribe.stream().map(stream -> CatalogHelpers
                        .createAirbyteStream(stream, Field.of("value", JsonSchemaType.STRING))
                        .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL)))
                .collect(Collectors.toList());
        return new AirbyteCatalog().withStreams(streams);
    }

    @Override
    public AutoCloseableIterator<AirbyteMessage> read(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) throws Exception {

//        new SystemAuthenticator();
        int numberOfConsumers =config.has("consumer_threads") ? config.get("consumer_threads").asInt(): 1;
        int threadPoolSize = numberOfConsumers + 3;
        ScheduledExecutorService ses = Executors.newScheduledThreadPool(threadPoolSize);

        Map<String, Object> additionalProperties = catalog.getAdditionalProperties();
        ConfiguredAirbyteStream configuredAirbyteStream = catalog.getStreams().get(0);
        ((ObjectNode) config).put("stream_name",configuredAirbyteStream.getStream().getName());
        String serverURL = additionalProperties.containsKey("bicycleServerURL") ? additionalProperties.get("bicycleServerURL").toString() : "";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String token = additionalProperties.containsKey("bicycleToken") ? additionalProperties.get("bicycleToken").toString() : "";
        String connectorId = additionalProperties.containsKey("bicycleConnectorId") ? additionalProperties.get("bicycleConnectorId").toString() : "";
        String eventSourceType = additionalProperties.containsKey("bicycleEventSourceType") ? additionalProperties.get("bicycleEventSourceType").toString() : "";
        String tenantId = additionalProperties.containsKey("bicycleTenantId") ? additionalProperties.get("bicycleTenantId").toString() : "tenantId";;
        String isOnPrem = additionalProperties.get("isOnPrem").toString();
        boolean isOnPremDeployment = Boolean.parseBoolean(isOnPrem);

        BicycleConfig bicycleConfig = new BicycleConfig(serverURL, token, connectorId, uniqueIdentifier, tenantId,  systemAuthenticator, isOnPremDeployment);
        setBicycleEventProcessor(bicycleConfig);
        EventSourceInfo eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);

        try {
    //      ses.scheduleAtFixedRate(metricAsEventsGenerator, 60, 300, TimeUnit.SECONDS);
            LOGGER.info("======Starting read operation for consumer " + bicycleConfig.getUniqueIdentifier() + "=======");
            final AirbyteConnectionStatus check = check(config);
            if (check.getStatus().equals(AirbyteConnectionStatus.Status.FAILED)) {
                LOGGER.info("Check Failure " + bicycleConfig.getUniqueIdentifier() + "=======");
                throw new RuntimeException("Unable establish a connection: " + check.getMessage());
            }
            LOGGER.info("Check Successful " + bicycleConfig.getUniqueIdentifier() + "=======");
//                totalRecords Read details required

            KinesisClientConfig kinesisClientConfig = new KinesisClientConfig(config,this);
            Scheduler scheduler =  kinesisClientConfig.getScheduler(bicycleConfig, eventSourceInfo);

            LOGGER.info("Created Kinesis Scheduler successfully " + bicycleConfig.getUniqueIdentifier() + "=======");

            for (int i = 0; i < numberOfConsumers; i++) {
                Thread schedulerThread = new Thread(scheduler);
                schedulerThread.setDaemon(true);
                schedulerThread.start();
            }
        } catch (Exception exception) {
            LOGGER.error("Shutting down the kinesis Client application", exception);
            ses.shutdown();
        }
        return null;
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
        return null;
    }

    public static void main(final String[] args) throws Exception {

        final Source source = new KinesisSource(null, null);
        LOGGER.info("Starting source: {}", KinesisSource.class);
        new IntegrationRunner(source).run(args);
        LOGGER.info("Completed source: {}", KinesisSource.class);
    }

}


