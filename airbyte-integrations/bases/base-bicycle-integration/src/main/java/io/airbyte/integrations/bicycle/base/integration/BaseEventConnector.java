package io.airbyte.integrations.bicycle.base.integration;

import static io.airbyte.integrations.bicycle.base.integration.CommonConstants.CONNECTOR_CONVERT_RECORDS_RAW_EVENTS;
import static io.airbyte.integrations.bicycle.base.integration.CommonConstants.CONNECTOR_PROCESS_RAW_EVENTS;
import static io.airbyte.integrations.bicycle.base.integration.CommonConstants.CONNECTOR_PROCESS_RAW_EVENTS_WITH_RULES_DOWNLOAD;
import static io.airbyte.integrations.bicycle.base.integration.CommonConstants.CONNECTOR_PUBLISH_EVENTS;
import static io.airbyte.integrations.bicycle.base.integration.CommonConstants.CONNECTOR_USER_SERVICE_RULES_DOWNLOAD;
import static io.airbyte.integrations.bicycle.base.integration.MetricAsEventsGenerator.SOURCE_TYPE;
import static io.bicycle.integration.common.bicycleconfig.BicycleConfig.SAAS_API_ROLE;
import static io.bicycle.integration.common.constants.EventConstants.SOURCE_ID;
import ai.apptuit.ml.utils.MetricUtils;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.inception.common.client.ServiceLocator;
import com.inception.common.client.impl.GenericApiClient;
import com.inception.schemastore.client.SchemaStoreApiClient;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.config.Config;
import com.inception.server.config.ConfigReference;
import com.inception.server.config.api.ConfigNotFoundException;
import com.inception.server.config.api.ConfigStoreException;
import com.inception.server.configstore.client.ConfigStoreAPIClient;
import com.inception.server.configstore.client.ConfigStoreClient;
import com.inception.server.entitystore.client.EntityStoreApiClient;
import com.inception.server.scheduler.api.JobExecutionStatus;
import com.inception.tenant.client.TenantServiceAPIClient;
import com.inception.tenant.query.TenantInfo;
import com.inception.traces.web.TraceQueryClient;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.Source;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.ai.model.tenant.summary.discovery.*;
import io.bicycle.blob.store.client.BlobStoreApiClient;
import io.bicycle.blob.store.client.BlobStoreClient;
import io.bicycle.blob.store.schema.BlobObject;
import io.bicycle.entity.mapping.api.ConnectionServiceClient;
import io.bicycle.event.processor.ConfigHelper;
import io.bicycle.event.processor.api.BicycleEventProcessor;
import io.bicycle.event.processor.impl.BicycleEventProcessorImpl;
import io.bicycle.event.publisher.api.BicycleEventPublisher;
import io.bicycle.event.publisher.impl.BicycleEventPublisherImpl;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.integration.common.Status;
import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.integration.common.config.BlackListedFields;
import io.bicycle.integration.common.config.ConnectorConfigService;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.integration.common.services.config.ConnectorConfigServiceImpl;
import io.bicycle.integration.common.utils.BlobStoreBroker;
import io.bicycle.integration.connector.*;
import io.bicycle.modelling.service.v2.DataUploadStatus;
import io.bicycle.server.verticalcontext.tenant.api.VerticalIdentifier;
import io.bicycle.tenant.ai.client.TenantSummaryDiscovererClient;
import io.bicycle.integration.common.transformation.TransformationImpl;
import io.bicycle.integration.common.utils.CommonUtil;
import io.bicycle.integration.common.utils.MetricUtilWrapper;
import io.bicycle.integration.common.writer.Writer;
import io.bicycle.integration.common.writer.WriterFactory;
import io.bicycle.integration.connector.runtime.BackFillConfiguration;
import io.bicycle.integration.connector.runtime.RuntimeConfig;
import io.bicycle.server.event.mapping.UserServiceMappingRule;
import io.bicycle.server.event.mapping.config.EventMappingConfigurations;
import io.bicycle.server.event.mapping.constants.BicycleEventPublisherType;
import io.bicycle.server.event.mapping.models.converter.BicycleEventsResult;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.models.publisher.EventPublisherResult;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;

import java.io.File;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author sumitmaheshwari
 * Created on 28/05/2022
 */
public abstract class BaseEventConnector extends BaseConnector implements Source {
    private static final int MAX_RETRY = 3;
    private static final int CONNECT_TIMEOUT_IN_MILLIS = 60000;
    private static final int READ_TIMEOUT_IN_MILLIS = 60000;
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final ConfigHelper configHelper = new ConfigHelper();
    protected ConfigStoreClient configStoreClient;
    protected SchemaStoreApiClient schemaStoreApiClient;
    protected EntityStoreApiClient entityStoreApiClient;
    protected TenantSummaryDiscovererClient tenantSummaryDiscovererClient;
    protected BlobStoreBroker blobStoreBroker;
    protected BlobStoreClient blobStoreClient;
    protected TenantServiceAPIClient tenantServiceApiClient;
    protected CommonUtil commonUtil = new CommonUtil();

    protected ObjectMapper mapper = new ObjectMapper();
    protected ConnectorConfigService connectorConfigService;
    private BicycleEventProcessor bicycleEventProcessor;
    protected BicycleEventPublisher bicycleEventPublisher;
    protected TransformationImpl dataTransformer;
    protected BicycleConfig bicycleConfig;
    protected SystemAuthenticator systemAuthenticator;
    protected EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier;
    private ConnectorConfigManager connectorConfigManager;
    protected BlackListedFields blackListedFields;
    protected static final String TENANT_ID = "tenantId";
    protected String ENV_TENANT_ID_KEY = "TENANT_ID";
    private static final String CONNECTORS_WITH_WAIT_ENABLED = "CONNECTORS_WITH_WAIT_ENABLED";
    private static final String CONNECTORS_WAIT_TIME_IN_MILLIS = "CONNECTORS_WAIT_TIME_IN_MILLIS";
    private static final int MAX_RETRY_COUNT = 3;
    protected static final int BATCH_SIZE = 30;
    private static final String PREVIEW_STORE_VALID_RECORDS = "PREVIEW_STORE_VALID_RECORDS";
    private static final String PREVIEW_STORE_INVALID_RECORDS = "PREVIEW_STORE_INVALID_RECORDS";


    protected List<String> listOfConnectorsWithSleepEnabled = new ArrayList<>();

    protected EventSourceInfo eventSourceInfo;

    protected ObjectMapper objectMapper = new ObjectMapper();
    protected JsonNode config;
    protected ConfiguredAirbyteCatalog catalog;
    protected Map<String, Object> additionalProperties;
    protected JsonNode state;

    protected ConnectionServiceClient connectionServiceClient;

    public RuntimeConfig getRuntimeConfig() {
        return runtimeConfig;
    }

    protected RuntimeConfig runtimeConfig;

    public Logger getLogger() {
        return logger;
    }

    public BaseEventConnector(SystemAuthenticator systemAuthenticator,
                              EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier,
                              ConnectorConfigManager connectorConfigManager, EventSourceInfo eventSourceInfo) {
        this.systemAuthenticator = systemAuthenticator;
        this.eventConnectorJobStatusNotifier = eventConnectorJobStatusNotifier;
        this.connectorConfigManager = connectorConfigManager;
        logger.info("ConnectorConfigManager [{}]", connectorConfigManager);
        String envConnectorsUsingPreviewStore =
                CommonUtil.getFromEnvironment(CONNECTORS_WITH_WAIT_ENABLED, false);
        if (!StringUtils.isEmpty(envConnectorsUsingPreviewStore)) {
            String[] connectorsWithSleepEnabled = envConnectorsUsingPreviewStore.split(",");
            listOfConnectorsWithSleepEnabled = Arrays.asList(connectorsWithSleepEnabled);
        } else {
            listOfConnectorsWithSleepEnabled.add("ad2e5fb0-4218-462c-8f5d-9dc76f5ac9b6");
        }
        this.eventSourceInfo = eventSourceInfo;
    }
    public BaseEventConnector(SystemAuthenticator systemAuthenticator,
                              EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier,
                              ConnectorConfigManager connectorConfigManager) {
        this(systemAuthenticator, eventConnectorJobStatusNotifier, connectorConfigManager, null);
    }

    public ConnectorConfigManager getConnectorConfigManager() {
        return connectorConfigManager;
    }

    public int getDelayInProcessing(final BackFillConfiguration backFillConfiguration) {
        if (backFillConfiguration.getEnableBackFill()) {
            return backFillConfiguration.getDelayInSecs();
        }
        return 0;
    }

    public boolean shouldContinue(BackFillConfiguration backFillConfiguration, long timestampInMillis) {

        if (!backFillConfiguration.getEnableBackFill()) {
            return true;
        }

        long startTime = backFillConfiguration.getStartTimeInMillis();
        long endTime = backFillConfiguration.getEndTimeInMillis();

        if (startTime == 0 && endTime == 0) {
            return true;
        }

        if (timestampInMillis >= startTime) {
            if (endTime != 0 && timestampInMillis <= endTime) {
                return true;
            } else if (endTime == 0) {
                return true;
            }
        }

        return false;
    }

    public EventConnectorJobStatusNotifier getEventConnectorJobStatusNotifier() {
        return eventConnectorJobStatusNotifier;
    }

    protected void initialize(JsonNode config, ConfiguredAirbyteCatalog catalog) {
        this.config = config;
        this.additionalProperties = catalog.getAdditionalProperties();
        this.blobStoreBroker = new BlobStoreBroker(getBlobStoreClient());
        this.tenantServiceApiClient = getTenantServiceApiClient();
        this.bicycleConfig = getBicycleConfig(additionalProperties, systemAuthenticator);
        setBicycleEventProcessorAndPublisher(bicycleConfig);
        this.state = getStateAsJsonNode(getAuthInfo(), getConnectorId());
        getConnectionServiceClient();
        this.connectorConfigService = new ConnectorConfigServiceImpl(configStoreClient, schemaStoreApiClient,
                entityStoreApiClient, null, null,
                null, systemAuthenticator, blobStoreBroker, null, null);
    }

    protected void submitRecordsToPreviewStore(String eventSourceId, List<RawEvent> rawEvents, boolean shouldFlush) {
        String eventSourceType = getEventSourceType(additionalProperties);
        EventSourceInfo eventSourceInfo = new EventSourceInfo(eventSourceId, eventSourceType);
        bicycleEventPublisher.publishPreviewEvents(getAuthInfo(), eventSourceInfo, rawEvents, shouldFlush);
        logger.info("[{}] : Published preview events [{}] [{}]", getConnectorId(), eventSourceId,
                shouldFlush);
    }

    protected void submitRecordsToPreviewStoreWithMetadata(String eventSourceId, List<RawEvent> rawEvents) {
        String eventSourceType = getEventSourceType(additionalProperties);
        EventSourceInfo eventSourceInfo = new EventSourceInfo(eventSourceId, eventSourceType);
        bicycleEventPublisher.publishMetadataPreviewEvents(getAuthInfo(), eventSourceInfo, rawEvents);
        logger.info("[{}] : Published preview events [{}] [{}]", getConnectorId(), eventSourceId);
    }

    protected void updateTenantSummaryVC(AuthInfo authInfo, String traceId, String companyName,
                                       List<RawEvent> rawEvents,
                                       String configId, String configName) {
        try {
            VerticalIdentifier verticalIdentifier = getVerticalIdentifier(companyName);

            RawDataKnowledgeBase.Builder rawDataKnowledgeBaseBuilder = RawDataKnowledgeBase.newBuilder();
            KnowledgeBaseMetadata knowledgeBaseMetadata = KnowledgeBaseMetadata.newBuilder()
                    .setCompanyName(companyName)
                    .setSource(io.bicycle.server.verticalcontext.tenant.api.Source.RAW_DATA)
                    .setSourceId(configId)
                    .setSourceName(configName)
                    .setConnectorId(configId)
                    .build();

            rawDataKnowledgeBaseBuilder.setMetaData(knowledgeBaseMetadata);
            rawDataKnowledgeBaseBuilder.setVerticalIdentifier(verticalIdentifier);

            Map<String, Set<String>> fieldsVsSamples = new HashMap<>();
            for (RawEvent rawEvent : rawEvents) {
                ObjectNode objectNode = (ObjectNode) rawEvent.getRawEventObject();
                objectNode.fields().forEachRemaining(entry -> {
                    String key = entry.getKey();
                    JsonNode value = entry.getValue();
                    if (!key.startsWith("bicycle")) {
                        if (value.isTextual()) {
                            fieldsVsSamples.computeIfAbsent(key, (k) -> new HashSet<>()).add(value.textValue());
                        } else if (value.isInt()) {
                            fieldsVsSamples.computeIfAbsent(key, (k) -> new HashSet<>()).add(String.valueOf(value.intValue()));
                        } else if (value.isDouble()) {
                            fieldsVsSamples.computeIfAbsent(key, (k) -> new HashSet<>()).add(String.valueOf(value.doubleValue()));
                        } else if (value.isBoolean()) {
                            fieldsVsSamples.computeIfAbsent(key, (k) -> new HashSet<>()).add(String.valueOf(value.booleanValue()));
                        }
                    }
                });
            }

            RawDataSource.Builder rawDataSourceBuilder = RawDataSource.newBuilder();
            //String topicName = rawEvent.getEventTypeName();
            //rawDataSourceBuilder.setSourceName(topicName);
            for (String fieldName : fieldsVsSamples.keySet()) {
                if (StringUtils.isEmpty(fieldName)) {
                    continue;
                }
                rawDataSourceBuilder.addDimensions(RawDataField.newBuilder()
                        .setFieldName(fieldName)
                        .addAllSampleValues(fieldsVsSamples.get(fieldName))
                        .build());
            }
            rawDataKnowledgeBaseBuilder.addRawDataSource(rawDataSourceBuilder.build());

            DiscoverTenantSummary.Builder builder = DiscoverTenantSummary.newBuilder();
            builder.setVertical(verticalIdentifier);
            builder.setRawDataKnowledgeBase(rawDataKnowledgeBaseBuilder.build());
            builder.setTraceInfo(io.bicycle.server.verticalcontext.tenant.api.TraceInfo
                    .newBuilder().setTraceId(traceId).build());

            DiscoverTenantSummaryResponse response =
                    tenantSummaryDiscovererClient.discoverTenantSummary(authInfo, builder.build());

            logger.info("{} Response from tenant summary discoverer {}", traceId, response);
        } catch (Exception e) {
            logger.error("{} Unable to update tenant summary for company {} because of {}", traceId, companyName, e);
        }
    }

    private VerticalIdentifier getVerticalIdentifier(String companyName) {
        return VerticalIdentifier.newBuilder().setVertical(companyName).setCompanyName(companyName).build();
    }

    public void createTenantVC(List<RawEvent> rawEvents) {
        ConfiguredConnectorStream connectorStream = getConfiguredConnectorStream(getAuthInfo(), getConnectorId());
        String name = connectorStream.getName();
        TenantInfo tenantInfo = tenantServiceApiClient.getTenantInfo(getAuthInfo(), getTenantId());
        updateTenantSummaryVC(getAuthInfo(), "", tenantInfo.getName(), rawEvents, getConnectorId(), name);
    }

    protected boolean processAndPublishEvents(List<RawEvent> rawEvents) {
        EventSourceInfo eventSourceInfo = new EventSourceInfo(getConnectorId(), getEventSourceType());
        EventProcessorResult eventProcessorResult = convertRawEventsToBicycleEvents(getAuthInfo(),
                eventSourceInfo, rawEvents);
        boolean publishEvents = true;
        publishEvents = publishEvents(getAuthInfo(), eventSourceInfo, eventProcessorResult);
        return publishEvents;
    }


    protected Map<String, String> readFilesConfig() {
        Map<String, String> fileNameVsSignedUrl = new HashMap<>();
        String traceInfo = "";
        ConfiguredConnectorStream connectorStream = getConfiguredConnectorStream(getAuthInfo(), getConnectorId());
        Pair namespaceAndUploadIds = commonUtil.getNameSpaceToUploadIdsForKnowledgeBaseConnector(traceInfo, connectorStream);
        logger.info("{} Fetch the namespace and uploadIds {}", traceInfo, namespaceAndUploadIds);
        if (namespaceAndUploadIds != null) {
            String namespace = (String) namespaceAndUploadIds.getLeft();
            Collection<String> uploadIds = (Collection<String>) namespaceAndUploadIds.getRight();

            for (String uploadId : uploadIds) {
                BlobObject fileMetadata = getFileMetadata(getAuthInfo(), traceInfo, namespace, uploadId);
                logger.info("{} Got the file metadata {}", traceInfo, fileMetadata);
                String signedUrl = getSingedUrl(getAuthInfo(), traceInfo, namespace, uploadId);
                if (StringUtils.isEmpty(signedUrl)) {
                    logger.warn("{} Unable to get the signed url for file {}", traceInfo,
                            fileMetadata.getName());
                    continue;
                }
                logger.info("{} Got the signed url {} for file {}", traceInfo, signedUrl,
                        fileMetadata.getName());
                fileNameVsSignedUrl.put(fileMetadata.getName(), signedUrl);
                //LOGGER.info("{} Got the file content {}", traceInfo, fileContent);
            }
        }
        return fileNameVsSignedUrl;
    }

    protected ConfiguredConnectorStream getConfiguredConnectorStream(AuthInfo authInfo, String configurationId) {
        return connectorConfigService.getConnectorStreamConfigById(authInfo, configurationId);
    }

    protected File storeFile(String fileName, String signedUrl) {
        try {
            File file = File.createTempFile(UUID.randomUUID().toString(), ".csv");
            file.deleteOnExit();
            final JsonNode provider = config.get("provider");

            if (provider !=null && provider.get("storage").asText().equals("GCS")) {
                //csvConnector.storeToFile(config, file);
            } else {
                FileUtils.copyURLToFile(new URL(signedUrl), file, CONNECT_TIMEOUT_IN_MILLIS, READ_TIMEOUT_IN_MILLIS);
            }
            return file;
        } catch (Exception e) {
            throw new RuntimeException("Unable to read file from GCS", e);
        }
    }

    private BlobObject getFileMetadata(AuthInfo authInfo, String traceInfo,
                                       String namespace, String uploadId) {
        return blobStoreBroker.getFileMetadata(authInfo, traceInfo, uploadId, namespace);
    }

    private String getFileContent(String traceInfo, String signedUrl) {
        return blobStoreBroker.getUploadFileContentAsString(traceInfo, signedUrl);
    }

    public String getSingedUrl(AuthInfo authInfo, String traceInfo, String namespace, String connectorUploadId) {
        try {
            return blobStoreBroker.getSingedUrl(authInfo, traceInfo, connectorUploadId, namespace);
        } catch (Throwable var6) {
            String message = "Failed to get signed url from blob store for given bicycle upload id";
            logger.error("{},{} {}", new Object[] {traceInfo, message, connectorUploadId, var6});
        }
        return null;
    }

    protected BlobStoreClient getBlobStoreClient() {
        if (blobStoreClient == null) {
            String serverUrl = getBicycleServerURL();
            if (serverUrl == null || serverUrl.isEmpty()) {
                throw new IllegalStateException("Bicycle server url is null");
            }
            ServiceLocator serviceLocator = new ServiceLocator() {
                @Override
                public String getBaseUri() {
                    return serverUrl;
                }
            };

            blobStoreClient = new BlobStoreApiClient(new GenericApiClient(), serviceLocator);
        }

        return blobStoreClient;
    }

    protected TenantServiceAPIClient getTenantServiceApiClient() {
        if (tenantServiceApiClient == null) {
            String serverUrl = getBicycleServerURL();
            if (serverUrl == null || serverUrl.isEmpty()) {
                throw new IllegalStateException("Bicycle server url is null");
            }
            ServiceLocator serviceLocator = new ServiceLocator() {
                @Override
                public String getBaseUri() {
                    return serverUrl;
                }
            };

            tenantServiceApiClient = new TenantServiceAPIClient(new GenericApiClient(), serviceLocator);
        }

        return tenantServiceApiClient;
    }

    abstract protected int getTotalRecordsConsumed();

    public void setBicycleEventProcessorAndPublisher(BicycleConfig bicycleConfig) {
        try {
            this.bicycleConfig = bicycleConfig;
            AuthInfo authInfo = bicycleConfig.getAuthInfo();
            configStoreClient = getConfigClient(bicycleConfig);
            schemaStoreApiClient = getSchemaStoreApiClient(bicycleConfig);
            entityStoreApiClient = getEntityStoreApiClient(bicycleConfig);
            tenantSummaryDiscovererClient = getTenantSummaryDiscovererClient(bicycleConfig);
            dataTransformer
                    = new TransformationImpl(schemaStoreApiClient, entityStoreApiClient, configStoreClient,
                    getTraceQueryClient(bicycleConfig), new MetricUtilWrapper());
            this.bicycleEventProcessor =
                    new BicycleEventProcessorImpl(
                            BicycleEventPublisherType.BICYCLE_EVENTS,
                            configStoreClient,
                            schemaStoreApiClient,
                            entityStoreApiClient,
                            dataTransformer
                    );
            EventMappingConfigurations eventMappingConfigurations =
                    new EventMappingConfigurations(
                            bicycleConfig.getServerURL(),
                            bicycleConfig.getMetricStoreURL(),
                            bicycleConfig.getServerURL(),
                            bicycleConfig.getEventURL(),
                            bicycleConfig.getServerURL(),
                            bicycleConfig.getTraceQueryUrl(),
                            bicycleConfig.getServerURL(),
                            bicycleConfig.getServerURL()
                    );
            logger.info("EventMappingConfiguration:: {}", eventMappingConfigurations);
            this.bicycleEventPublisher = new BicycleEventPublisherImpl(eventMappingConfigurations, systemAuthenticator,
                    true, dataTransformer, connectorConfigManager);
        } catch (Throwable e) {
            logger.error("Exception while setting bicycle event process and publisher", e);
        }
    }

    public JsonRawEvent createJsonRawEvent(JsonNode jsonNode) {
        return new JsonRawEvent(jsonNode, dataTransformer);
    }

    public JsonRawEvent createJsonRawEvent(String json) {
        return new JsonRawEvent(json, dataTransformer);
    }

    static ConfigStoreClient getConfigClient(BicycleConfig bicycleConfig) {
        return new ConfigStoreAPIClient(new GenericApiClient(), new ServiceLocator() {
            @Override
            public String getBaseUri() {
                return bicycleConfig.getServerURL();
            }
        }, new ServiceLocator() {
            @Override
            public String getBaseUri() {
                return bicycleConfig.getServerURL();
            }
        }, null) {
            @Override
            public Config getLatest(AuthInfo authInfo, ConfigReference ref)
                    throws ConfigStoreException, ConfigNotFoundException {

                return super.getLatest(authInfo, ref);
            }
        };
    }

    private static SchemaStoreApiClient getSchemaStoreApiClient(BicycleConfig bicycleConfig) {
        return new SchemaStoreApiClient(new GenericApiClient(), new ServiceLocator() {
            @Override
            public String getBaseUri() {
                return bicycleConfig.getServerURL();
            }
        });
    }

    private static TenantSummaryDiscovererClient getTenantSummaryDiscovererClient(BicycleConfig bicycleConfig) {
        return new TenantSummaryDiscovererClient(new GenericApiClient(), new ServiceLocator() {
            @Override
            public String getBaseUri() {
                return bicycleConfig.getServerURL();
            }
        });
    }

    private static TraceQueryClient getTraceQueryClient(BicycleConfig bicycleConfig) {
        return new TraceQueryClient(bicycleConfig.getTraceQueryUrl());
    }

    private static EntityStoreApiClient getEntityStoreApiClient(BicycleConfig bicycleConfig) {
        return new EntityStoreApiClient(new GenericApiClient(), new ServiceLocator() {
            @Override
            public String getBaseUri() {
                return bicycleConfig.getServerURL();
            }
        });
    }

    public abstract void stopEventConnector();

    public void stopEventConnector(String message, JobExecutionStatus jobExecutionStatus) {
        if (eventConnectorJobStatusNotifier.getSchedulesExecutorService() != null) {
            eventConnectorJobStatusNotifier.getSchedulesExecutorService().shutdown();
        }
        eventConnectorJobStatusNotifier.removeConnectorInstanceFromMap(bicycleConfig.getConnectorId());
        AuthInfo authInfo = bicycleConfig.getAuthInfo();
        eventConnectorJobStatusNotifier.sendStatus(jobExecutionStatus,message, bicycleConfig.getConnectorId(), getTotalRecordsConsumed(), authInfo);
        logger.info(message + " for connector {}", bicycleConfig.getConnectorId());
    }

    public List<RawEvent> convertRecordsToRawEvents(List<?> records) {
        Timer.Context timer = MetricUtils.getMetricRegistry().timer(
                CONNECTOR_CONVERT_RECORDS_RAW_EVENTS
                        .withTags(SOURCE_ID, bicycleConfig.getConnectorId())
                        .toString()
        ).time();
        List<RawEvent> rawEvents = convertRecordsToRawEventsInternal(records);
        timer.stop();
        return rawEvents;
    }

    public abstract List<RawEvent> convertRecordsToRawEventsInternal(List<?> records);

    public abstract AutoCloseableIterator<AirbyteMessage> preview(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) throws InterruptedException, ExecutionException;

    public SyncDataResponse syncData(JsonNode sourceConfig,
                                     ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                                     JsonNode readState,
                                     SyncDataRequest syncDataRequest) {

        String traceInfo = CommonUtil.getTraceInfo(syncDataRequest.getTraceInfo());
        Map<String, Object> additionalProperties = configuredAirbyteCatalog.getAdditionalProperties();

        logger.info("Got sync data request for sync data request {}, additional Properties {}", syncDataRequest,
                additionalProperties);

        String eventSourceType = getEventSourceType(additionalProperties);
        String connectorId = getConnectorId(additionalProperties);
        String sourceId = toUUID(connectorId);
        BicycleConfig bicycleConfig = getBicycleConfig(additionalProperties, systemAuthenticator);
        setBicycleEventProcessorAndPublisher(bicycleConfig);
        EventSourceInfo eventSourceInfo = new EventSourceInfo(sourceId, eventSourceType);
        long startTime = System.currentTimeMillis() - (24 * 60 * 60 * 1000); //last 1 day
        long endTime = System.currentTimeMillis();

        AuthInfo authInfo = getAuthInfo();

        long limit =  syncDataRequest.getSyncDataCountLimit() + 10;
        List<RawEvent> rawEvents = bicycleEventPublisher
                .getPreviewEvents(authInfo, eventSourceInfo, limit, startTime, endTime);

        if (rawEvents.size() > 0) {
            Writer writer = WriterFactory.getWriter(syncDataRequest.getSyncDestination());
            processAndSync(authInfo, traceInfo,
                    syncDataRequest.getConfiguredConnectorStream().getConfiguredConnectorStreamId(),
                    eventSourceInfo, System.currentTimeMillis(), writer, rawEvents, false);
            logger.info("Received {} events from preview store for sync data request {} and event source info {}",
                    rawEvents.size(), syncDataRequest, eventSourceInfo);
            return null;
        }
        logger.info("Received no events from preview store for sync data request {}, event source info {} " +
                        "and it took {} ms", syncDataRequest, eventSourceInfo, System.currentTimeMillis() - endTime);
        return null;
    }

    public String toUUID(String configId) {
        if (StringUtils.isBlank(configId)) {
            return null;
        } else {
            String[] split = StringUtils.split(configId, ":");
            if (split.length == 1) {
                return split[0];
            } else if (split.length == 2) {
                return split[1];
            }
            return null;
        }
    }

    public EventProcessorResult convertRawEventsToBicycleEvents(AuthInfo authInfo,
                                                                EventSourceInfo eventSourceInfo,
                                                                List<RawEvent> rawEvents) {

        Timer.Context timer = MetricUtils.getMetricRegistry().timer(
                CONNECTOR_PROCESS_RAW_EVENTS_WITH_RULES_DOWNLOAD
                        .withTags(SOURCE_ID, eventSourceInfo.getEventSourceId())
                        .withTags(SOURCE_TYPE, eventSourceInfo.getEventSourceType())
                        .toString()
        ).time();
        EventProcessorResult eventProcessorResult =
                bicycleEventProcessor.processEvents(authInfo, eventSourceInfo, rawEvents);
        timer.stop();
        return eventProcessorResult;

    }
    public EventProcessorResult convertRawEventsToBicycleEvents(AuthInfo authInfo,
                                                               EventSourceInfo eventSourceInfo,
                                                               List<RawEvent> rawEvents,
                                                                List<UserServiceMappingRule> userServiceMappingRules) {


        Timer.Context timer = MetricUtils.getMetricRegistry().timer(
                CONNECTOR_PROCESS_RAW_EVENTS
                        .withTags(SOURCE_ID, eventSourceInfo.getEventSourceId())
                        .withTags(SOURCE_TYPE, eventSourceInfo.getEventSourceType())
                        .toString()
        ).time();
        EventProcessorResult eventProcessorResult =
                bicycleEventProcessor.processEvents(authInfo, eventSourceInfo, rawEvents, userServiceMappingRules);
        timer.stop();

        return eventProcessorResult;

    }

    public boolean publishEvents(AuthInfo authInfo, EventSourceInfo eventSourceInfo,
                                 BicycleEventsResult bicycleEventsResult){

        if (bicycleEventsResult.getBicycleEvents().getEventsList().size() == 0) {
            return true;
        }

        return bicycleEventPublisher.publishEvents(authInfo, eventSourceInfo, bicycleEventsResult);
    }

    public boolean publishDummyEvents(AuthInfo authInfo, EventSourceInfo eventSourceInfo, long durationInSeconds) {
        return bicycleEventPublisher.publishDummyEvents(authInfo, eventSourceInfo, durationInSeconds, 5000);
    }
    public boolean publishEvents(AuthInfo authInfo, EventSourceInfo eventSourceInfo,
                                 EventProcessorResult eventProcessorResult) {

        Timer.Context timer = MetricUtils.getMetricRegistry().timer(
                CONNECTOR_PUBLISH_EVENTS
                        .withTags(SOURCE_ID, eventSourceInfo.getEventSourceId())
                        .withTags(SOURCE_TYPE, eventSourceInfo.getEventSourceType())
                        .toString()
        ).time();
        if (eventProcessorResult == null) {
            return true;
        }
        int retry = 0;
        EventPublisherResult publisherResult = null;

        while (retry < MAX_RETRY) {

            publisherResult = bicycleEventPublisher.publishEvents(authInfo, eventSourceInfo,
                    eventProcessorResult);

            if (publisherResult != null) {
                return true;
            }
            retry++;
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
        }

        timer.stop();

        if (publisherResult == null) {
            logger.warn("There was some issue in publishing events");
            return false;
        }

        return true;
    }

    public void processAndSync(AuthInfo authInfo,
                               String traceInfo,
                               String configuredConnectorStreamId,
                               EventSourceInfo eventSourceInfo,
                               long readTimestamp,
                               Writer writer,
                               List<RawEvent> rawEvents,
                               boolean saveAsPreviewEvents) {
        try {
            ProcessRawEventsResult processedEvents = this.processRawEvents(authInfo, eventSourceInfo, rawEvents);
            try {

                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("Processed Event Result Size for traceInfo " + traceInfo + " is " +
                         processedEvents.getProcessedEventSourceDataCount());
                stringBuilder.append("\n");
                for (ProcessedEventSourceData processRawEventsResult:
                        processedEvents.getProcessedEventSourceDataList()) {
                    stringBuilder.append("Connector Stream:: " + configuredConnectorStreamId);
                    stringBuilder.append("\n");
                    stringBuilder.append("Raw Event:: " + processRawEventsResult.getRawEvent());
                    stringBuilder.append("\n");
                    stringBuilder.append("BicycleEvent:: " + processRawEventsResult.getBicycleEvent());
                    logger.info(stringBuilder.toString());
                    break;
                }

                writer.writeEventData(
                        configuredConnectorStreamId, readTimestamp, processedEvents.getProcessedEventSourceDataList());
                if (saveAsPreviewEvents) {
                    savePreviewEvents(authInfo, traceInfo, eventSourceInfo, processedEvents);
                }
            } catch (Exception e) {
                logger.error(traceInfo + " Exception while writing processed events to destination", e);
            }
        } catch (Exception e) {
            logger.error(traceInfo + " Exception while processing raw events", e);
        }
    }

    public boolean doesMappingRulesExists(AuthInfo authInfo, EventSourceInfo eventSourceInfo) {
        List<UserServiceMappingRule> userServiceMappingRules =
                this.configHelper.getUserServiceMappingRules(
                        authInfo,
                        eventSourceInfo.getEventSourceId(),
                        configStoreClient
                );

        return userServiceMappingRules != null ? userServiceMappingRules.size() > 0 ? true : false : false;
    }
    private ProcessRawEventsResult processRawEvents(AuthInfo authInfo,
                                                   EventSourceInfo eventSourceInfo,
                                                   List<RawEvent> rawEvents) {
        List<UserServiceMappingRule> userServiceMappingRules =
                this.configHelper.getUserServiceMappingRules(
                        authInfo,
                        eventSourceInfo.getEventSourceId(),
                        configStoreClient
                );
        return bicycleEventProcessor.processAndGenerateBicycleEvents(
                authInfo, eventSourceInfo, rawEvents, userServiceMappingRules);
    }

    public List<UserServiceMappingRule> getUserServiceMappingRules(AuthInfo authInfo, EventSourceInfo eventSourceInfo) {

        Timer.Context timer = MetricUtils.getMetricRegistry().timer(
                CONNECTOR_USER_SERVICE_RULES_DOWNLOAD
                        .withTags(SOURCE_ID, bicycleConfig.getConnectorId())
                        .withTags(SOURCE_TYPE, eventSourceInfo.getEventSourceType())
                        .toString()
        ).time();

        List<UserServiceMappingRule> rules = this.configHelper.getUserServiceMappingRules(
                authInfo,
                eventSourceInfo.getEventSourceId(),
                configStoreClient
        );
        timer.stop();
        return rules;
    }

    public String getTenantId() {
        return this.bicycleConfig.getTenantId();
    }

    public EventSourceInfo getEventSourceInfo() {
        return eventSourceInfo;
    }

    private void savePreviewEvents(AuthInfo authInfo,
                                   String traceInfo,
                                   EventSourceInfo eventSourceInfo,
                                   ProcessRawEventsResult processRawEventsResult) {
        try {
            List<RawEvent> rawEvents = new ArrayList<>();
            for (ProcessedEventSourceData processedEventSourceData:
                    processRawEventsResult.getProcessedEventSourceDataList()) {
                rawEvents.add(new JsonRawEvent(processedEventSourceData.getRawEvent()));
            }
            logger.debug(traceInfo + " Preview bicycle events for event source "
                    + eventSourceInfo + rawEvents);
            if (this.bicycleEventPublisher.publishPreviewEvents(authInfo, eventSourceInfo, rawEvents, true)) {
                logger.info(traceInfo + " Successfully published preview events for event source " + eventSourceInfo);
            } else {
                logger.warn(traceInfo + " Failed to publish preview events for event source " + eventSourceInfo);
            }
        } catch (Exception e) {
            logger.warn(traceInfo + " Exception while writing preview events", e);
        }
    }

    public AutoCloseableIterator<AirbyteMessage> read(JsonNode config, ConfiguredAirbyteCatalog catalog,
                                                      JsonNode state)  throws Exception {
        int retry = 3;
        while (retry > 0) {
            try {
                this.config = config;
                this.catalog = catalog;
                this.additionalProperties = catalog.getAdditionalProperties();
                BicycleConfig bicycleConfig = getBicycleConfig();
                setBicycleEventProcessorAndPublisher(bicycleConfig);
                getConnectionServiceClient();
                this.state = getStateAsJsonNode(getAuthInfo(), getConnectorId());
                return doRead(config, catalog, state);
            } catch (Throwable e) {
                logger.error("{}, Error while trying to perform read, connector read will be retried, retries " +
                        "remaining {}", bicycleConfig != null ? bicycleConfig.getConnectorId() : null, retry, e);
                retry -= 1;
            }
        }
        this.stopEventConnector("Shutting down the event Connector after 3 retries", JobExecutionStatus.failure);
        return null;
    }

    public abstract AutoCloseableIterator<AirbyteMessage> doRead(final JsonNode config,
                                                                 final ConfiguredAirbyteCatalog catalog,
                                                                 final JsonNode state) throws Exception;

    public String getEventSourceType() {
        return additionalProperties.containsKey("bicycleEventSourceType") ?
                additionalProperties.get("bicycleEventSourceType").toString() : CommonUtils.UNKNOWN_EVENT_CONNECTOR;
    }

    protected String getConnectorId() {
        return additionalProperties.containsKey("bicycleConnectorId") ?
                additionalProperties.get("bicycleConnectorId").toString() : "";
    }

    protected String getConnectorConfigurationId() {
        return additionalProperties.containsKey("bicycleConnectorConfigurationId") ?
                additionalProperties.get("bicycleConnectorConfigurationId").toString() : "";
    }

    private BicycleConfig getBicycleConfig() {
        String serverURL = getBicycleServerURL();
        String metricStoreURL = additionalProperties.containsKey("bicycleMetricStoreURL") ? additionalProperties.get("bicycleMetricStoreURL").toString() : "";
        String token = additionalProperties.containsKey("bicycleToken") ? additionalProperties.get("bicycleToken").toString() : "";
        String connectorId = getConnectorId();
        String uniqueIdentifier = UUID.randomUUID().toString();
        String tenantId = additionalProperties.containsKey("bicycleTenantId") ? additionalProperties.get("bicycleTenantId").toString() : "tenantId";
        String isOnPrem = additionalProperties.containsKey("isOnPrem") ? additionalProperties.get("isOnPrem").toString() : "false";
       // String isOnPrem = additionalProperties.get("isOnPrem").toString();
        boolean isOnPremDeployment = Boolean.parseBoolean(isOnPrem);
        return new BicycleConfig(serverURL, metricStoreURL, token, connectorId, uniqueIdentifier, tenantId,
                systemAuthenticator, isOnPremDeployment);
    }

    protected String getStateAsString(String key) {
        return state != null && state.get(key) != null ? state.get(key).asText() : null;
    }

    protected boolean getStateAsBoolean(String key) {
        return state != null && state.get(key) != null ? state.get(key).asBoolean() : false;
    }

    protected long getStateAsLong(String key) {
        return state != null && state.get(key) != null ? state.get(key).asLong() : -1;
    }

    protected Status getConnectorSyncStatus() throws InvalidProtocolBufferException {
        JsonNode syncStatus = getState().get("syncStatus");
        DataUploadStatus.Builder builder = DataUploadStatus.newBuilder();
        if (syncStatus != null) {
            String value = syncStatus.textValue();
            JsonFormat.parser().ignoringUnknownFields().merge(value, builder);
            DataUploadStatus dataUploadStatus = builder.build();
            return dataUploadStatus.getStatus();
        }
        return null;
    }

    protected void updateConnectorSyncState(Status status)
            throws JsonProcessingException, InvalidProtocolBufferException {
        JsonNode syncStatus = getState().get("syncStatus");
        DataUploadStatus.Builder builder = DataUploadStatus.newBuilder();
        if (syncStatus != null) {
            String value = syncStatus.textValue();
            JsonFormat.parser().ignoringUnknownFields().merge(value, builder);
            builder.setStatus(status);
            String jsonString = JsonFormat.printer().print(builder.build());
            saveState("syncStatus", jsonString);
        } else {
            updateConnectorSyncState(status, 0);
        }
    }

    protected void updateConnectorSyncState(Status status, double progress)
            throws JsonProcessingException, InvalidProtocolBufferException {
        DataUploadStatus dataUploadStatus = DataUploadStatus.newBuilder()
                .setStatus(status)
                .setProgress(progress)
                .build();
        String jsonString = JsonFormat.printer().print(dataUploadStatus);
        saveState("syncStatus", jsonString);
    }

    protected void saveState(String key, String value) throws JsonProcessingException {
        JsonNode state = getState();
        JsonNode oldValue = state.get(key);
        ((ObjectNode)state).put(key, value);
        String payload = objectMapper.writeValueAsString(state);
        if (!upsertState(payload)) {
            if (oldValue != null) {
                ((ObjectNode) state).put(key, oldValue.asText());
            } else {
                ((ObjectNode) state).remove(key);
            }
        }
    }

    public JsonNode getUpdatedState(String key, long value) {
        ObjectNode state = objectMapper.createObjectNode();
        state.put(key, value);
        return state;
    }

    protected void saveState(String key, long value) throws JsonProcessingException {
        JsonNode state = getState();
        JsonNode oldValue = state.get(key);
        ((ObjectNode)state).put(key, value);
        String payload = objectMapper.writeValueAsString(state);
        if (!upsertState(payload)) {
            if (oldValue != null) {
                ((ObjectNode) state).put(key, oldValue.asLong());
            } else {
                ((ObjectNode) state).remove(key);
            }
        }
    }

    protected void saveState(String key, boolean value) throws JsonProcessingException {
        JsonNode state = getState();
        JsonNode oldValue = state.get(key);
        ((ObjectNode)state).put(key, value);
        String payload = objectMapper.writeValueAsString(state);
        if (!upsertState(payload)) {
            if (oldValue != null) {
                ((ObjectNode) state).put(key, oldValue.asBoolean());
            } else {
                ((ObjectNode) state).remove(key);
            }
        }
    }

    protected JsonNode getStateAsJsonNode(AuthInfo authInfo, String streamId) {
        try {
            String state = connectionServiceClient.getReadStateConfigById(authInfo, streamId);
            if (state == null) {
                return null;
            }
            return objectMapper.readTree(state);
        } catch (Throwable e) {
            logger.error("Unable to get state as json node for stream {} {}", streamId, e);
        }

        return null;
    }

    protected void setState(JsonNode state) {
        this.state = state;
    }

    public AirbyteStateMessage getState(AuthInfo authInfo, String streamId) {

        try {
            String state = connectionServiceClient.getReadStateConfigById(authInfo, streamId);
            if (!StringUtils.isEmpty(state)) {
                AirbyteStateMessage airbyteMessage = objectMapper.readValue(state, AirbyteStateMessage.class);
                return airbyteMessage;
            }
        }catch (Throwable e) {
            logger.error("Unable to get state for streamId " + streamId, e);
        }

        return null;
    }

    protected String getStateAsString(AuthInfo authInfo, String streamId) {

        try {
            String state = connectionServiceClient.getReadStateConfigById(authInfo, streamId);
            if (StringUtils.isEmpty(state) || state.equals("null")) {
                return null;
            }
            return state;
        } catch (Throwable e) {
            logger.error("Unable to get state for streamId " + streamId, e);
        }

        return null;
    }

    public boolean setState(AuthInfo authInfo, String streamId, JsonNode jsonNode) {

        try {
            logger.info("Setting state for stream {} {}", streamId, jsonNode);
            AirbyteStateMessage airbyteMessage = new AirbyteStateMessage();
            airbyteMessage.setData(jsonNode);
            String airbyteMessageAsString = objectMapper.writeValueAsString(airbyteMessage);
            int counter = 0;
            boolean success = false;
            Exception ex = null;
            while (counter < MAX_RETRY_COUNT) {
                try {
                    connectionServiceClient.upsertReadStateConfig(authInfo, streamId, airbyteMessageAsString);
                    logger.info("Successfully set state for stream {}", streamId);
                    success = true;
                    break;
                } catch (Exception e) {
                    ex = e;
                    counter++;
                }
            }
            if (!success) {
                logger.error("Unable to set state for streamId " + streamId, ex);
            }
            return success;
        } catch (Throwable e) {
            logger.error("Unable to set state for streamId " + streamId, e);
        }

        return false;
    }

    public boolean setStateAsString(AuthInfo authInfo, String streamId, JsonNode jsonNode) {

        try {
            logger.info("Setting state for stream {} {}", streamId, jsonNode);
            String airbyteMessageAsString = objectMapper.writeValueAsString(jsonNode);
            int counter = 0;
            boolean success = false;
            Exception ex = null;
            while (counter < MAX_RETRY_COUNT) {
                try {
                    connectionServiceClient.upsertReadStateConfig(authInfo, streamId, airbyteMessageAsString);
                    logger.info("Successfully set state for stream {}", streamId);
                    success = true;
                    break;
                } catch (Exception e) {
                    ex = e;
                    counter++;
                }
            }
            if (!success) {
                logger.error("Unable to set state for streamId " + streamId, ex);
            }
            return success;
        } catch (Throwable e) {
            logger.error("Unable to set state for streamId " + streamId, e);
        }

        return false;
    }

    private JsonNode getState() {
        if (state == null) {
            state = objectMapper.createObjectNode();
        } else if (state.isEmpty()) {
            state = objectMapper.createObjectNode();
        }
        return state;
    }

    private boolean upsertState(String payload) {
        int retries = 0;
        do {
            try {
                connectionServiceClient.upsertReadStateConfig(getAuthInfo(), getConnectorId(), payload);
                return true;
            } catch (Exception e) {
                logger.error("Exception updating the status [{}] [{}] [{}]", getTenantId(), getConnectorId(), payload, e);
                try {
                    Thread.sleep(1000);
                    retries++;
                } catch (InterruptedException ex) {
                }
            }
        } while (retries < 5);
        return false;
    }

    public AuthInfo getAuthInfo() {
        return bicycleConfig.getAuthInfo(SAAS_API_ROLE);
    }

    protected ConnectionServiceClient getConnectionServiceClient() {
        if (connectionServiceClient == null) {
            String serverUrl = getBicycleServerURL();
            if (serverUrl == null || serverUrl.isEmpty()) {
                throw new IllegalStateException("Bicycle server url is null");
            }
            connectionServiceClient = new ConnectionServiceClient(new GenericApiClient(), serverUrl);
        }
        return connectionServiceClient;
    }

    protected String getBicycleServerURL() {
        String serverURL = additionalProperties.containsKey("bicycleServerURL") ?
                additionalProperties.get("bicycleServerURL").toString() : "";
        return serverURL;
    }

    protected String getEventSourceType(Map<String, Object> additionalProperties) {
        return additionalProperties.containsKey("bicycleEventSourceType") ?
                additionalProperties.get("bicycleEventSourceType").toString() : CommonUtils.UNKNOWN_EVENT_CONNECTOR;
    }

    protected String getConnectorId(Map<String, Object> additionalProperties) {
        return additionalProperties.containsKey("bicycleConnectorId") ?
                additionalProperties.get("bicycleConnectorId").toString() : "";
    }

    protected BicycleConfig getBicycleConfig(Map<String, Object> additionalProperties,
                                           SystemAuthenticator systemAuthenticator) {
        String serverURL = additionalProperties.containsKey("bicycleServerURL") ? additionalProperties.get("bicycleServerURL").toString() : "";
        String metricStoreURL = additionalProperties.containsKey("bicycleMetricStoreURL") ? additionalProperties.get("bicycleMetricStoreURL").toString() : "";
        String token = additionalProperties.containsKey("bicycleToken") ? additionalProperties.get("bicycleToken").toString() : "";
        String connectorId = getConnectorId(additionalProperties);
        String uniqueIdentifier = UUID.randomUUID().toString();
        String tenantId = additionalProperties.containsKey("bicycleTenantId") ? additionalProperties.get("bicycleTenantId").toString() : "tenantId";
        String isOnPrem = additionalProperties.get("isOnPrem").toString();
        boolean isOnPremDeployment = Boolean.parseBoolean(isOnPrem);
        BicycleConfig bicycleConfig = new BicycleConfig(serverURL, metricStoreURL, token, connectorId, uniqueIdentifier, tenantId,
                systemAuthenticator, isOnPremDeployment);
        runtimeConfig = this.getConnectorConfigManager() != null ? this.getConnectorConfigManager().getRuntimeConfig(bicycleConfig.getAuthInfo(), connectorId) : null;
        if (runtimeConfig != null && connectorConfigManager.isDefaultConfig(runtimeConfig)) {
            runtimeConfig = null;
        }
        return bicycleConfig;
    }

    protected void publishPreviewEvents(File file, EventSourceReader<RawEvent> reader, List<RawEvent> vcEvents,
                                            int maxRecords, int totalRecords,
                                            boolean saveState, boolean shouldFlush, boolean updateVC)
            throws Exception {
        try {
            List<RawEvent> validEvents = new ArrayList<>();
            List<RawEvent> inValidEvents = new ArrayList<>();
            int count = 0;
            int valid_count = 0;
            int invalid_count = 0;
            while(reader.hasNext()) {
                RawEvent next = null;
                try {
                    next = reader.next();
                    if (reader.isValidEvent()) {
                        validEvents.add(next);
                        if (updateVC) {
                            vcEvents.add(next);
                        }
                        valid_count++;
                    } else {
                        inValidEvents.add(next);
                        invalid_count++;
                    }
                } catch (Exception e) {
                    if (next != null) {
                        inValidEvents.add(next);
                    }
                    invalid_count++;
                }

                count++;
                if (validEvents.size() >= BATCH_SIZE) {
                    submitRecordsToPreviewStore(getConnectorId(), validEvents, shouldFlush);
                    validEvents.clear();
                    if (saveState) {
                        updateConnectorSyncState(Status.IN_PROGRESS, (double) count/ (double) totalRecords);
                    }
                }
                if (inValidEvents.size() >= BATCH_SIZE) {
                    submitRecordsToPreviewStoreWithMetadata(getConnectorId(), inValidEvents);
                    inValidEvents.clear();
                }
                if (valid_count >= maxRecords) {
                    break;
                }
            }
            logger.info("[{}] : Raw events total - Total Count [{}] Valid[{}] Invalid[{}]",
                    getConnectorId(), file.getName(), valid_count, invalid_count);
            submitRecordsToPreviewStore(getConnectorId(), validEvents, shouldFlush);
            submitRecordsToPreviewStoreWithMetadata(getConnectorId(), inValidEvents);
            if (saveState) {
                updateConnectorSyncState(Status.IN_PROGRESS, (double) count/ (double) totalRecords);
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    public static class NonEmptyAutoCloseableIterator implements AutoCloseableIterator {

        @Override
        public void close() throws Exception {

        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Object next() {
            return null;
        }
    }

    public static abstract class EventSourceReader<T> {

        public abstract boolean hasNext();

        public abstract boolean isValidEvent();

        public abstract long getRecordUTCTimestampInMillis();
        public abstract T next();

        public abstract void close() throws Exception;

    }

}
