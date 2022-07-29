package io.airbyte.integrations.source.kinesis;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.BicycleConfig;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.*;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.kinesis.coordinator.Scheduler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;


public class KinesisSource extends BaseEventConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisSource.class);
    private static final String TENANT_ID = "tenantId";
    public KinesisSource(SystemAuthenticator systemAuthenticator, EventConnectorJobStatusNotifier eventConnectorStatusHandler) {
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

        int numberOfConsumers =config.has("consumer_threads") ? config.get("consumer_threads").asInt(): 1;
        int threadPoolSize = numberOfConsumers + 3;

        createBicycleConfigFromConfigAndCatalog(config, catalog, state);

        ConfiguredAirbyteStream configuredAirbyteStream = catalog.getStreams().get(0);
        ((ObjectNode) config).put("stream_name",configuredAirbyteStream.getStream().getName());

        ScheduledExecutorService ses = Executors.newScheduledThreadPool(threadPoolSize);
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
        Iterator<String> recordsIterator = (Iterator<String>) records.iterator();
        List<RawEvent> rawEvents = new ArrayList<>();
        while (recordsIterator.hasNext()) {
            String record = recordsIterator.next();
            JsonRawEvent jsonRawEvent = new JsonRawEvent(record);
            rawEvents.add(jsonRawEvent);
        }
        if (rawEvents.size() == 0) {
            return null;
        }
        return rawEvents;
    }

    @Override
    public AutoCloseableIterator<AirbyteMessage> preview(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) {
        AmazonKinesis kinesis;
        GetRecordsRequest recordsRequest;
        String name="KinesisConsumerTask";
        ConfiguredAirbyteStream configuredAirbyteStream = catalog.getStreams().get(0);
        ((ObjectNode) config).put("stream_name",configuredAirbyteStream.getStream().getName());
        String stream = config.has("stream_name") ? config.get("stream_name").asText() : "";
        String accessKey = config.has("access_key") ? config.get("access_key").asText() : "";
        String secretKey = config.has("private_key") ? config.get("private_key").asText() : "";
        String regionString = config.has("region") ? config.get("region").asText() : "";
        Region region = Region.of(ObjectUtils.firstNonNull(regionString, "us-east-2"));
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        kinesis = AmazonKinesisClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCredentials)).withRegion(Regions.AP_SOUTH_1).build();
        List<Shard> shards = null;
        try {
            StreamDescription streamDesc = kinesis.describeStream(stream).getStreamDescription();
            shards = streamDesc.getShards();
        }
        catch (Exception e) {
            LOGGER.error("Exception while trying to get shard list",e);
        }

        recordsRequest = new GetRecordsRequest();
        GetShardIteratorRequest readShardsRequest = new GetShardIteratorRequest();
        readShardsRequest.setStreamName(stream);
        readShardsRequest.setShardIteratorType(ShardIteratorType.LATEST);
        readShardsRequest.setShardId(((Shard) shards.toArray()[0]).getShardId());

        GetShardIteratorResult shardIterator = kinesis.getShardIterator(readShardsRequest);
        recordsRequest.setShardIterator(shardIterator.getShardIterator());

        try {
            GetRecordsResult recordsResult = kinesis.getRecords(recordsRequest);
            List<Record> records = recordsResult.getRecords();
            List<UserRecord> deaggregatedRecords = UserRecord.deaggregate(records);
            Iterator<UserRecord> userRecordIterator = deaggregatedRecords.stream().iterator();
            recordsRequest.setShardIterator(recordsResult.getNextShardIterator());
            ObjectMapper objectMapper = new ObjectMapper();

            return AutoCloseableIterators.fromIterator(new AbstractIterator<>() {

                @Override
                protected AirbyteMessage computeNext() {
                    if (userRecordIterator.hasNext()) {
                        final Record record = userRecordIterator.next();
                        JsonNode data = null;
                        try {
                            data = objectMapper.readTree(record.getData().array());
                        } catch (IOException e) {
                            LOGGER.error("Cannot convert Record to Json",e);
                        }
                        return new AirbyteMessage()
                                .withType(AirbyteMessage.Type.RECORD)
                                .withRecord(new AirbyteRecordMessage()
                                        .withStream(stream)
                                        .withEmittedAt(Instant.now().toEpochMilli())
                                        .withData(data));
                    }

                    return endOfData();
                }

            });
        }
        catch (Exception e) {
            LOGGER.error("Exception while fetching records",e);
        }
        return null;
    }

    public static void main(final String[] args) throws Exception {

        final Source source = new KinesisSource(null, null);
        LOGGER.info("Starting source: {}", KinesisSource.class);
        new IntegrationRunner(source).run(args);
        LOGGER.info("Completed source: {}", KinesisSource.class);
    }

}


