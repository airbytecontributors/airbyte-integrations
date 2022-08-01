package io.airbyte.integrations.source.kinesis;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.BicycleConfig;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.*;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.Scheduler;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class KinesisSource extends BaseEventConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisSource.class);
    private static final String TENANT_ID = "tenantId";
    private final Map<String, Map<String, Long>> clientToStreamShardRecordsRead = new HashMap<>();
    protected KinesisClientConfig kinesisClientConfig;
    private String applicationName;
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
        AmazonKinesis kinesis = kinesisClientConfig.getCheckClient();
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
        String metricStoreURL = additionalProperties.containsKey("bicycleMetricStoreURL") ? additionalProperties.get("bicycleMetricStoreURL").toString() : "";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String token = additionalProperties.containsKey("bicycleToken") ? additionalProperties.get("bicycleToken").toString() : "";
        String connectorId = additionalProperties.containsKey("bicycleConnectorId") ? additionalProperties.get("bicycleConnectorId").toString() : "";
        String eventSourceType = additionalProperties.containsKey("bicycleEventSourceType") ? additionalProperties.get("bicycleEventSourceType").toString() : "";
        String tenantId = additionalProperties.containsKey("bicycleTenantId") ? additionalProperties.get("bicycleTenantId").toString() : "tenantId";;
        String isOnPrem = additionalProperties.get("isOnPrem").toString();
        boolean isOnPremDeployment = Boolean.parseBoolean(isOnPrem);

        BicycleConfig bicycleConfig = new BicycleConfig(serverURL, metricStoreURL,token, connectorId, uniqueIdentifier, tenantId,  systemAuthenticator, isOnPremDeployment);
        setBicycleEventProcessor(bicycleConfig);
        EventSourceInfo eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);
        KinesisClientConfig kinesisClientConfig = new KinesisClientConfig(config,this);
        this.kinesisClientConfig= kinesisClientConfig;
        KinesisMetricAsEventsGenerator kinesisMetricAsEventsGenerator = new KinesisMetricAsEventsGenerator(bicycleConfig, eventSourceInfo, config, bicycleEventPublisher, this);
        AuthInfo authInfo = bicycleConfig.getAuthInfo();
        try {
            ses.scheduleAtFixedRate(kinesisMetricAsEventsGenerator, 60, 300, TimeUnit.SECONDS);
            LOGGER.info("======Starting read operation for consumer " + bicycleConfig.getUniqueIdentifier() + "=======");
            final AirbyteConnectionStatus check = check(config);
            if (check.getStatus().equals(AirbyteConnectionStatus.Status.FAILED)) {
                LOGGER.info("Check Failure " + bicycleConfig.getUniqueIdentifier() + "=======");
                throw new RuntimeException("Unable establish a connection: " + check.getMessage());
            }
            LOGGER.info("Check Successful " + bicycleConfig.getUniqueIdentifier() + "=======");
            ArrayList<Scheduler> schedulerArrayList = new ArrayList<Scheduler>();
            eventConnectorJobStatusNotifier.setNumberOfThreadsRunning(new AtomicInteger(numberOfConsumers));
            eventConnectorJobStatusNotifier.setScheduledExecutorService(ses);
            for (int i = 0; i < numberOfConsumers; i++) {
                Map<String, Long> totalRecordsRead = new HashMap<>();
                String clientThreadId = UUID.randomUUID().toString();
                clientToStreamShardRecordsRead.put(clientThreadId,totalRecordsRead);
                Scheduler scheduler =  kinesisClientConfig.getScheduler(bicycleConfig, eventSourceInfo, false, null, totalRecordsRead);
                LOGGER.info("Created Kinesis Scheduler successfully for UUID " + bicycleConfig.getUniqueIdentifier() + "=======");
                schedulerArrayList.add(scheduler);
                Thread schedulerThread = new Thread(scheduler);
                schedulerThread.setDaemon(true);
                schedulerThread.start();
            }
            eventConnectorJobStatusNotifier.sendStatus(JobExecutionStatus.processing,"Kinesis Event Connector started Successfully", connectorId, authInfo);
            this.shouldStop.await();
            for (Scheduler scheduler: schedulerArrayList) {
                scheduler.startGracefulShutdown().get();
            }
            eventConnectorJobStatusNotifier.getSchedulesExecutorService().shutdown();
            eventConnectorJobStatusNotifier.removeConnectorIdFromMap(eventSourceInfo.getEventSourceId());
            eventConnectorJobStatusNotifier.sendStatus(JobExecutionStatus.success,"Shutting down the Kinesis Event Connector manually", connectorId, authInfo);
        } catch (Exception exception) {
            eventConnectorJobStatusNotifier.getSchedulesExecutorService().shutdown();
            eventConnectorJobStatusNotifier.removeConnectorIdFromMap(eventSourceInfo.getEventSourceId());
            eventConnectorJobStatusNotifier.sendStatus(JobExecutionStatus.failure,"Shutting down the Kinesis Event Connector", connectorId, authInfo);
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
    public AutoCloseableIterator<AirbyteMessage> preview(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) throws InterruptedException, ExecutionException {

        ConfiguredAirbyteStream configuredAirbyteStream = catalog.getStreams().get(0);
        ((ObjectNode) config).put("stream_name",configuredAirbyteStream.getStream().getName());
        String stream = config.has("stream_name") ? config.get("stream_name").asText() : "";
        final String applicationName = config.has("application_name") ? config.get("application_name").asText() : "test-v2";
        final AirbyteConnectionStatus check = check(config);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        if (check.getStatus().equals(AirbyteConnectionStatus.Status.FAILED)) {
            LOGGER.info("Check Failure for config {} =======", config);
            throw new RuntimeException("Unable establish a connection: " + check.getMessage());
        }
        LOGGER.info("Check Successful for config {} =======", config);
//        List<String> objList = Collections.synchronizedList(new ArrayList<String>());
//        KinesisClientConfig kinesisClientConfig = new KinesisClientConfig(config,this);
//        Scheduler scheduler =  kinesisClientConfig.getScheduler(null, null, true, objList);
//
//        Thread schedulerThread = new Thread(scheduler);
//        schedulerThread.setDaemon(true);
//        schedulerThread.start();
//
//        while (true) {
//            TimeUnit.SECONDS.sleep(1);
//            if (objList.size()>10) {
//                CompletableFuture<Boolean> booleanCompletableFuture = scheduler.startGracefulShutdown();
//                try {
//                    booleanCompletableFuture.get();
//                } catch (ExecutionException e) {
//                }
//                schedulerThread.interrupt();
//                break;
//            }
//        }
//        Iterator<Object> userRecordIterator = objList.stream().iterator();
//        ObjectMapper objectMapper = new ObjectMapper();
//        return AutoCloseableIterators.fromIterator(new AbstractIterator<>() {
//
//            @Override
//            protected AirbyteMessage computeNext() {
//                if (userRecordIterator.hasNext()) {
//                    final String record = (String) userRecordIterator.next();
//                    JsonNode data = null;
//                    try {
//                        data = objectMapper.readTree(record);
//                    } catch (IOException e) {
//                        LOGGER.error("Cannot convert Record to Json",e);
//                    }
//                    return new AirbyteMessage()
//                            .withType(AirbyteMessage.Type.RECORD)
//                            .withRecord(new AirbyteRecordMessage()
//                                    .withStream(stream)
//                                    .withEmittedAt(Instant.now().toEpochMilli())
//                                    .withData(data));
//                }
//
//                return endOfData();
//            }
//
//        });
        final List<software.amazon.awssdk.services.kinesis.model.Record> objList = Collections.synchronizedList(new ArrayList<>());
        KinesisClientConfig kinesisClientConfig = new KinesisClientConfig(config,this);
        KinesisAsyncClient kinesisClient = kinesisClientConfig.getKinesisClient();
        CompletableFuture<ListShardsResponse> listShardsResponseCompletableFuture = kinesisClient.listShards(ListShardsRequest.builder().streamName(stream).build());
        ListShardsResponse listShardsResponse = listShardsResponseCompletableFuture.get();
        ArrayList<CompletableFuture<Void>> completeableFutureList = new ArrayList<>();
        CompletableFuture<DescribeStreamResponse> describeStreamResponseCompletableFuture = kinesisClient.describeStream(DescribeStreamRequest.builder().streamName(stream).build());
        String streamARN = describeStreamResponseCompletableFuture.get().streamDescription().streamARN();
        String consumerARN;
        try {
            RegisterStreamConsumerRequest registerStreamConsumerRequest = RegisterStreamConsumerRequest.builder().consumerName(applicationName).streamARN(streamARN).build();
            CompletableFuture<RegisterStreamConsumerResponse> registerStreamConsumerResponseCompletableFuture = kinesisClient.registerStreamConsumer(registerStreamConsumerRequest);
            RegisterStreamConsumerResponse registerStreamConsumerResponse = registerStreamConsumerResponseCompletableFuture.get();
            consumerARN = registerStreamConsumerResponse.consumer().consumerARN();
        }
        catch (Exception e) {
            CompletableFuture<DescribeStreamConsumerResponse> describeStreamConsumerResponseCompletableFuture = kinesisClient.describeStreamConsumer(DescribeStreamConsumerRequest.builder().streamARN(streamARN).consumerName(applicationName).build());
            consumerARN = describeStreamConsumerResponseCompletableFuture.get().consumerDescription().consumerARN();
        }
        int batchsize= Math.max(100/listShardsResponse.shards().size(), 20);
        String initialPositionInStream = config.get("initial_position_in_stream").asText();
        ShardIteratorType shardIteratorType;
        if (initialPositionInStream == "Latest") {
            shardIteratorType = ShardIteratorType.LATEST;
        }
        else {
            shardIteratorType = ShardIteratorType.TRIM_HORIZON;
        }
        for (software.amazon.awssdk.services.kinesis.model.Shard shard : listShardsResponse.shards()) {
            SubscribeToShardRequest subscribeToShardRequest = SubscribeToShardRequest.builder().shardId(shard.shardId()).consumerARN(consumerARN).startingPosition(StartingPosition.builder().type(shardIteratorType).build()).build();
            SubscribeToShardResponseHandler responseHandler = SubscribeToShardResponseHandler
                    .builder()
                    .onError(e -> LOGGER.error("Error during stream - " + e.getMessage()))
                    .onEventStream(publisher -> publisher
                            // Filter to only SubscribeToShardEvents
                            // Flat map into a publisher of just records
                            // Limit to 1000 total records
                            // Batch records into lists of 25
                            // Print out each record batch
                            .filter(SubscribeToShardEvent.class)
                            .flatMapIterable(SubscribeToShardEvent::records)
                            .limit(100)
                            .buffer(batchsize)
                            .subscribe(batch -> {
                                objList.addAll(batch);
                                if (objList.size()>=100) {
                                    countDownLatch.countDown();
                                }
                            }))
                    .build();
            completeableFutureList.add(kinesisClient.subscribeToShard(subscribeToShardRequest, responseHandler));
        }
        countDownLatch.await(15,TimeUnit.SECONDS);
        for (CompletableFuture<Void> future: completeableFutureList) {
            future.complete(null);
        }
        LOGGER.info("{} records read for Preview for Kinesis Connector with Config {}", objList.size(), config);
        Iterator<Record> userRecordIterator = objList.stream().iterator();
        ObjectMapper objectMapper = new ObjectMapper();
        return AutoCloseableIterators.fromIterator(new AbstractIterator<>() {

            @Override
            protected AirbyteMessage computeNext() {
                if (userRecordIterator.hasNext()) {
                    final Record record = (Record) userRecordIterator.next();
                    JsonNode data = null;
                    try {
                        data = objectMapper.readTree(record.data().asByteArray());
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

//        AmazonKinesis kinesis;
//        GetRecordsRequest recordsRequest;
//        String name="KinesisConsumerTask";
////        final List<UserRecord> objList = Collections.synchronizedList(new ArrayList<>());
//        String accessKey = config.has("access_key") ? config.get("access_key").asText() : "";
//        String secretKey = config.has("private_key") ? config.get("private_key").asText() : "";
//        String regionString = config.has("region") ? config.get("region").asText() : "";
//        Region region = Region.of(ObjectUtils.firstNonNull(regionString, "us-east-2"));
//        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
//        kinesis = AmazonKinesisClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCredentials)).withRegion(Regions.AP_SOUTH_1).build();
//        List<Shard> shards = null;
//        try {
//            StreamDescription streamDesc = kinesis.describeStream(stream).getStreamDescription();
//            shards = streamDesc.getShards();
//        }
//        catch (Exception e) {
//            LOGGER.error("Exception while trying to get shard list",e);
//        }
//
//        while (objList.size()<100) {
//            for (Shard shard: shards) {
//                try {
//
//                    recordsRequest = new GetRecordsRequest();
//                    GetShardIteratorRequest readShardsRequest = new GetShardIteratorRequest();
//                    readShardsRequest.setStreamName(stream);
//                    readShardsRequest.setShardIteratorType(ShardIteratorType.TRIM_HORIZON);
//                    readShardsRequest.setShardId((shard).getShardId());
//                    GetShardIteratorResult shardIterator = kinesis.getShardIterator(readShardsRequest);
//                    recordsRequest.setShardIterator(shardIterator.getShardIterator());
//                    GetRecordsResult recordsResult = kinesis.getRecords(recordsRequest);
//                    while (!recordsResult.getRecords().isEmpty()) {
//                        List<Record> records = recordsResult.getRecords();
//                        List<UserRecord> deaggregatedRecords = UserRecord.deaggregate(records);
//                        objList = Stream.of(deaggregatedRecords, objList)
//                                .flatMap(Collection::stream)
//                                .collect(Collectors.toList());
//                        recordsRequest.setShardIterator(recordsResult.getNextShardIterator());
//                        recordsResult = kinesis.getRecords(recordsRequest);
//                    }
//                } catch (Exception e) {
//                    LOGGER.error("Exception while fetching records", e);
//                    return null;
//                }
//            }
//        }
//
//        Iterator<UserRecord> userRecordIterator = objList.stream().iterator();
//        ObjectMapper objectMapper = new ObjectMapper();
//        return AutoCloseableIterators.fromIterator(new AbstractIterator<>() {
//
//            @Override
//            protected AirbyteMessage computeNext() {
//                if (userRecordIterator.hasNext()) {
//                    final UserRecord record = (UserRecord) userRecordIterator.next();
//                    JsonNode data = null;
//                    try {
//                        data = objectMapper.readTree(record.getData().array());
//                    } catch (IOException e) {
//                        LOGGER.error("Cannot convert Record to Json",e);
//                    }
//                    return new AirbyteMessage()
//                            .withType(AirbyteMessage.Type.RECORD)
//                            .withRecord(new AirbyteRecordMessage()
//                                    .withStream(stream)
//                                    .withEmittedAt(Instant.now().toEpochMilli())
//                                    .withData(data));
//                }
//
//                return endOfData();
//            }
//
//        });
    }

    public static void main(final String[] args) throws Exception {

        final Source source = new KinesisSource(null, null);
        LOGGER.info("Starting source: {}", KinesisSource.class);
        new IntegrationRunner(source).run(args);
        LOGGER.info("Completed source: {}", KinesisSource.class);
    }

    public Map<String, Map<String, Long>> getClientToStreamShardRecordsRead() {
        return clientToStreamShardRecordsRead;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }
}


