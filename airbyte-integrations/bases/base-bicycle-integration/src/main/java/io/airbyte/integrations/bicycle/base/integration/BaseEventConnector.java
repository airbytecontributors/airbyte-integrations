package io.airbyte.integrations.bicycle.base.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.Source;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.entity.mapping.api.ConnectionServiceClient;
import io.bicycle.event.processor.ConfigHelper;
import io.bicycle.event.processor.api.BicycleEventProcessor;
import io.bicycle.event.processor.impl.BicycleEventProcessorImpl;
import io.bicycle.event.publisher.api.BicycleEventPublisher;
import io.bicycle.event.publisher.impl.BicycleEventPublisherImpl;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.integration.common.transformation.TransformationImpl;
import io.bicycle.integration.common.utils.CommonUtil;
import io.bicycle.integration.common.writer.Writer;
import io.bicycle.integration.common.writer.WriterFactory;
import io.bicycle.integration.connector.ProcessRawEventsResult;
import io.bicycle.integration.connector.ProcessedEventSourceData;
import io.bicycle.integration.connector.SyncDataRequest;
import io.bicycle.server.event.mapping.UserServiceMappingRule;
import io.bicycle.server.event.mapping.config.EventMappingConfigurations;
import io.bicycle.server.event.mapping.constants.BicycleEventPublisherType;
import io.bicycle.server.event.mapping.models.converter.BicycleEventsResult;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.models.publisher.EventPublisherResult;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;

import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.airbyte.integrations.bicycle.base.integration.BicycleConfig.SAAS_API_ROLE;

/**
 * @author sumitmaheshwari
 * Created on 28/05/2022
 */
public abstract class BaseEventConnector extends BaseConnector implements Source {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final ConfigHelper configHelper = new ConfigHelper();
    private ConfigStoreClient configStoreClient;
    private SchemaStoreApiClient schemaStoreApiClient;
    private EntityStoreApiClient entityStoreApiClient;
    private BicycleEventProcessor bicycleEventProcessor;
    protected BicycleEventPublisher bicycleEventPublisher;
    protected BicycleConfig bicycleConfig;
    protected SystemAuthenticator systemAuthenticator;
    protected EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier;
    protected static final String TENANT_ID = "tenantId";
    protected String ENV_TENANT_ID_KEY = "TENANT_ID";
    private static final String CONNECTORS_WITH_WAIT_ENABLED = "CONNECTORS_WITH_WAIT_ENABLED";
    private static final String CONNECTORS_WAIT_TIME_IN_MILLIS = "CONNECTORS_WAIT_TIME_IN_MILLIS";

    protected List<String> listOfConnectorsWithSleepEnabled = new ArrayList<>();
    protected long sleepTimeInMillis = 5000;
    protected EventSourceInfo eventSourceInfo;

    protected ObjectMapper objectMapper = new ObjectMapper();
    protected JsonNode config;
    protected ConfiguredAirbyteCatalog catalog;
    protected Map<String, Object> additionalProperties;
    protected JsonNode state;

    protected ConnectionServiceClient connectionServiceClient;
    public BaseEventConnector(SystemAuthenticator systemAuthenticator, EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier) {
        this.systemAuthenticator = systemAuthenticator;
        this.eventConnectorJobStatusNotifier = eventConnectorJobStatusNotifier;
        String envConnectorsUsingPreviewStore =
                CommonUtil.getFromEnvironment(CONNECTORS_WITH_WAIT_ENABLED, false);
        String sleepTime =
                CommonUtil.getFromEnvironment(CONNECTORS_WAIT_TIME_IN_MILLIS, false);
        try {
            sleepTimeInMillis = Long.parseLong(sleepTime);
        } catch (Exception e) {
            logger.info("CONNECTORS_WAIT_TIME_IN_MILLIS not set correctly");
        }
        if (!StringUtils.isEmpty(envConnectorsUsingPreviewStore)) {
            String[] connectorsWithSleepEnabled = envConnectorsUsingPreviewStore.split(",");
            listOfConnectorsWithSleepEnabled = Arrays.asList(connectorsWithSleepEnabled);
        } else {
            listOfConnectorsWithSleepEnabled.add("ad2e5fb0-4218-462c-8f5d-9dc76f5ac9b6");
        }
    }

    public long getSleepTimeInMillis() {
        return sleepTimeInMillis;
    }


    public boolean isSleepEnabledForConnector(final AuthInfo authInfo, final String connectorStreamUUID) {
        return listOfConnectorsWithSleepEnabled.contains(connectorStreamUUID)
                || listOfConnectorsWithSleepEnabled.contains(authInfo.getTenantId());
    }

    public EventConnectorJobStatusNotifier getEventConnectorJobStatusNotifier() {
        return eventConnectorJobStatusNotifier;
    }

    abstract protected int getTotalRecordsConsumed();

    public void setBicycleEventProcessorAndPublisher(BicycleConfig bicycleConfig) {
        try {
            this.bicycleConfig = bicycleConfig;
            configStoreClient = getConfigClient(bicycleConfig);
            schemaStoreApiClient = getSchemaStoreApiClient();
            entityStoreApiClient = getEntityStoreApiClient();
            this.bicycleEventProcessor =
                    new BicycleEventProcessorImpl(
                            BicycleEventPublisherType.BICYCLE_EVENTS,
                            configStoreClient,
                            schemaStoreApiClient,
                            entityStoreApiClient
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
                    true, new TransformationImpl());
        } catch (Throwable e) {
            logger.error("Exception while setting bicycle event process and publisher", e);
        }
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

    private static SchemaStoreApiClient getSchemaStoreApiClient() {
        return null;
    }

    private static EntityStoreApiClient getEntityStoreApiClient() {
        return null;
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

    public abstract List<RawEvent> convertRecordsToRawEvents(List<?> records);

    public abstract AutoCloseableIterator<AirbyteMessage> preview(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) throws InterruptedException, ExecutionException;

    public AutoCloseableIterator<AirbyteMessage> syncData(JsonNode sourceConfig,
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
            return new NonEmptyAutoCloseableIterator();
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

        EventProcessorResult eventProcessorResult =
                bicycleEventProcessor.processEvents(authInfo, eventSourceInfo, rawEvents);

        return eventProcessorResult;

    }

    public boolean publishEvents(AuthInfo authInfo, EventSourceInfo eventSourceInfo,
                                 EventProcessorResult eventProcessorResult) {

        if (eventProcessorResult == null) {
            return true;
        }
        EventPublisherResult publisherResult = bicycleEventPublisher.publishEvents(authInfo, eventSourceInfo, eventProcessorResult);

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
            BicycleEventsResult bicycleEventsResult = this.bicycleEventProcessor.processEventsForPreview(
                    authInfo, eventSourceInfo, rawEvents, new ArrayList<>());
            logger.debug(traceInfo + " Preview bicycle events for event source "
                    + eventSourceInfo + bicycleEventsResult.getUnmatchedBicycleEvents());
            if (this.bicycleEventPublisher.publishEvents(authInfo, eventSourceInfo, bicycleEventsResult)) {
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
        this.config = config;
        this.catalog = catalog;
        this.additionalProperties = catalog.getAdditionalProperties();
        this.state = state;
        BicycleConfig bicycleConfig = getBicycleConfig();
        setBicycleEventProcessorAndPublisher(bicycleConfig);
        getConnectionServiceClient();
        return doRead(config, catalog, state);
    }

    public AutoCloseableIterator<AirbyteMessage> doRead(JsonNode config, ConfiguredAirbyteCatalog catalog,
                                                      JsonNode state) throws Exception {
        return null;
    }

    protected String getEventSourceType() {
        return additionalProperties.containsKey("bicycleEventSourceType") ?
                additionalProperties.get("bicycleEventSourceType").toString() : CommonUtils.UNKNOWN_EVENT_CONNECTOR;
    }

    protected String getConnectorId() {
        return additionalProperties.containsKey("bicycleConnectorId") ?
                additionalProperties.get("bicycleConnectorId").toString() : "";
    }
    private BicycleConfig getBicycleConfig() {
        String serverURL = getBicycleServerURL();
        String metricStoreURL = additionalProperties.containsKey("bicycleMetricStoreURL") ? additionalProperties.get("bicycleMetricStoreURL").toString() : "";
        String token = additionalProperties.containsKey("bicycleToken") ? additionalProperties.get("bicycleToken").toString() : "";
        String connectorId = getConnectorId();
        String uniqueIdentifier = UUID.randomUUID().toString();
        String tenantId = additionalProperties.containsKey("bicycleTenantId") ? additionalProperties.get("bicycleTenantId").toString() : "tenantId";
        String isOnPrem = additionalProperties.get("isOnPrem").toString();
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

    protected AuthInfo getAuthInfo() {
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

    private String getBicycleServerURL() {
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
        return new BicycleConfig(serverURL, metricStoreURL, token, connectorId, uniqueIdentifier, tenantId,
                systemAuthenticator, isOnPremDeployment);
    }

    private static class NonEmptyAutoCloseableIterator implements AutoCloseableIterator {

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

}
