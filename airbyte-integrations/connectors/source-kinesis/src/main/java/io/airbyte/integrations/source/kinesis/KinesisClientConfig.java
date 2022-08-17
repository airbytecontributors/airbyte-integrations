package io.airbyte.integrations.source.kinesis;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.bicycle.base.integration.BicycleConfig;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.util.*;
import java.util.stream.Collectors;

public class KinesisClientConfig {
    KinesisAsyncClient kinesisClient;
    DynamoDbAsyncClient dynamoDbAsyncClient;
    CloudWatchAsyncClient cloudWatchClient;
    AmazonKinesis kinesis;
    private final KinesisSource kinesissource;
    protected final Logger LOGGER = LoggerFactory.getLogger(KinesisClientConfig.class);
    private JsonNode config;
    private String streamPattern;
    private String configuredStream;
    private String applicationName;
    public KinesisClientConfig(final JsonNode config, final KinesisSource kinesisSource) {
        this.config = config;
        this.kinesissource = kinesisSource;
        streamPattern = config.has("stream_pattern") ? config.get("stream_pattern").asText() : "";
        configuredStream = config.has("stream_name") ? config.get("stream_name").asText() : "";
        applicationName = config.has("application_name") ? config.get("application_name").asText() : "test-v2";
        kinesissource.setApplicationName(applicationName);

    }

    public KinesisAsyncClient getKinesisClient() {
        if (kinesisClient==null) {
            this.buildAllClients(config);
        }
        return kinesisClient;
    }

    public CloudWatchAsyncClient getCloudWatchClient() {
        if (cloudWatchClient==null) {
            this.buildAllClients(config);
        }
        return cloudWatchClient;
    }

    public DynamoDbAsyncClient getDynamodbConsumer() {
        if (dynamoDbAsyncClient==null) {
            this.buildAllClients(config);
        }
        return dynamoDbAsyncClient;
    }

    public Scheduler getScheduler(BicycleConfig bicycleConfig, EventSourceInfo eventSourceInfo, Boolean isPreview, List<Object> objList, Map<String, Long> totalRecordsRead) {
//        final String applicationName = "test-v2";
//        final String streamName = KinesisClientConfig.getKinesisClientConfig(config).getStreamPattern();

        final String streamName = getConfiguredStream();

        KinesisAsyncClient kinesisClient = getKinesisClient();
        DynamoDbAsyncClient dynamoClient = getDynamodbConsumer();
        CloudWatchAsyncClient cloudWatchClient = getCloudWatchClient();
        ConfigsBuilder configsBuilder;
        configsBuilder = new ConfigsBuilder(configuredStream, applicationName, kinesisClient, dynamoClient, cloudWatchClient, UUID.randomUUID().toString(), new ShardRecordProcessorFactoryImpl(kinesissource, bicycleConfig, eventSourceInfo, this, isPreview, objList, totalRecordsRead));
        /**
         * The Scheduler (also called Worker in earlier versions of the KCL) is the entry point to the KCL. This
         * instance is configured with defaults provided by the ConfigsBuilder.
         */

        RetrievalConfig retrievalConfig = setRetrievalConfig(configsBuilder, streamName, kinesisClient);
        Integer max_lease_renewal_threads=Integer.parseInt(config.has("max_lease_renewal_threads") ? config.get("max_lease_renewal_threads").asText() : "20");
        Integer max_leases_for_worker=Integer.parseInt(config.has("max_leases_for_worker") ? config.get("max_leases_for_worker").asText() : "2147483647");
        Integer failover_time_millis=Integer.parseInt(config.has("failover_time_millis") ? config.get("failover_time_millis").asText() : "1000");

        LeaseManagementConfig leaseManagementConfig = configsBuilder.leaseManagementConfig();
        leaseManagementConfig.maxLeaseRenewalThreads(max_lease_renewal_threads);
        leaseManagementConfig.maxLeasesForWorker(max_leases_for_worker);
        leaseManagementConfig.failoverTimeMillis(failover_time_millis);

        Scheduler scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                leaseManagementConfig,
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                retrievalConfig
        );
        return scheduler;
    }

    private RetrievalConfig setRetrievalConfig(ConfigsBuilder configsBuilder, String streamName, KinesisAsyncClient kinesisClient) {
        JsonNode config = this.config;
        String initialPositionInStream = config.get("initial_position_in_stream").asText();
        InitialPositionInStreamExtended initialPositionInStreamExtended = null;
        Integer max_getrecords_threadpool=Integer.parseInt(config.has("max_getrecords_threadpool") ? config.get("max_getrecords_threadpool").asText() : "4");
        Integer idle_time_between_reads_in_millis=Integer.parseInt(config.has("idle_time_between_reads_in_millis") ? config.get("idle_time_between_reads_in_millis").asText() : "1000");
        Integer retry_getrecords_in_seconds=Integer.parseInt(config.has("retry_getrecords_in_seconds") ? config.get("retry_getrecords_in_seconds").asText() : "0");
        Integer max_records=Integer.parseInt(config.has("max_records") ? config.get("max_records").asText() : "10000");

        if (initialPositionInStream.equals("Latest")) {
            initialPositionInStreamExtended = InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);
        }
        else {
            initialPositionInStreamExtended = InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
        }
        PollingConfig pollingConfig = new PollingConfig(streamName, kinesisClient);
        pollingConfig.idleTimeBetweenReadsInMillis(idle_time_between_reads_in_millis);
        pollingConfig.maxGetRecordsThreadPool(Optional.of(max_getrecords_threadpool));
        pollingConfig.retryGetRecordsInSeconds(Optional.of(retry_getrecords_in_seconds));
        pollingConfig.maxRecords(max_records);


        RetrievalConfig retrievalConfig = configsBuilder.retrievalConfig().retrievalSpecificConfig(pollingConfig);
        retrievalConfig.initialPositionInStreamExtended(initialPositionInStreamExtended);
        return retrievalConfig;
    }

    private KinesisAsyncClient buildAllClients(final JsonNode config) {
        final String streamName = config.has("stream_pattern") ? config.get("stream_pattern").asText() : "";
        final String accessKey = config.has("access_key") ? config.get("access_key").asText() : "";
        final String secretKey = config.has("private_key") ? config.get("private_key").asText() : "";
        final String regionString = config.has("region") ? config.get("region").asText() : "";
        try {
            Region region = Region.of(ObjectUtils.firstNonNull(regionString, "us-east-2"));
            AwsCredentialsProvider credentialProvider = new AwsCredentialsProvider() {
                @Override
                public AwsCredentials resolveCredentials() {
                    return new AwsCredentials() {
                        @Override
                        public String accessKeyId() {
                            return accessKey;
                        }

                        @Override
                        public String secretAccessKey() {
                            return secretKey;
                        }
                    };
                }
            };
//
            this.kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().credentialsProvider(credentialProvider).region(region));
            this.dynamoDbAsyncClient = DynamoDbAsyncClient.builder().credentialsProvider(credentialProvider).region(region).build();
            this.cloudWatchClient = CloudWatchAsyncClient.builder().credentialsProvider(credentialProvider).region(region).build();
            return kinesisClient;
        }
        catch (Exception e) {
            LOGGER.error("Exception attempting to create Kinesis Client for Check command: ", e);
        }
        return null;
    }

    public AmazonKinesis getCheckClient() {
        JsonNode config = this.config;
        final String streamName = config.has("stream_pattern") ? config.get("stream_pattern").asText() : "";
        final String accessKey = config.has("access_key") ? config.get("access_key").asText() : "";
        final String secretKey = config.has("private_key") ? config.get("private_key").asText() : "";
        final String regionString = config.has("region") ? config.get("region").asText() : "";
        Region region = Region.of(ObjectUtils.firstNonNull(regionString, "us-east-2"));

        try {
            BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
            AmazonKinesis kinesis = AmazonKinesisClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCredentials)).withRegion(String.valueOf(region)).build();
            this.kinesis=kinesis;
            LOGGER.info("Created Kinesis Check Client successfully");
            return kinesis;
        }
        catch (Exception e) {
            LOGGER.error("Exception attempting to create Kinesis Client for Check command: ", e);
        }
        return null;
    }

    public String getStreamPattern() {
        return streamPattern;
    }

    public String getConfiguredStream() {
        return configuredStream;
    }

    public Set<String> getStreamsToSubscribe(String streamNamePattern) {
        ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
        ListStreamsResult listStreamsResult = kinesis.listStreams(listStreamsRequest);
        List<String> streamNames = listStreamsResult.getStreamNames();
        Set<String> streamsToSubscribe = streamNames.stream().filter(topic -> topic.matches(streamNamePattern)).collect(Collectors.toSet());
        return streamsToSubscribe;
    }

}
