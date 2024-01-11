package io.airbyte.integrations.bicycle.base.integration;

import static io.bicycle.integration.common.bicycleconfig.BicycleConfig.SAAS_API_ROLE;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.inception.traces.web.TraceQueryClient;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.Source;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.blob.store.client.BlobStoreApiClient;
import io.bicycle.blob.store.client.BlobStoreClient;
import io.bicycle.entity.mapping.api.ConnectionServiceClient;
import io.bicycle.event.processor.ConfigHelper;
import io.bicycle.event.processor.api.BicycleEventProcessor;
import io.bicycle.event.publisher.api.BicycleEventPublisher;
import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.integration.common.config.BlackListedFields;
import io.bicycle.integration.common.config.ConnectorConfigService;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.integration.common.services.config.ConnectorConfigServiceImpl;
import io.bicycle.integration.common.utils.BlobStoreBroker;
import io.bicycle.integration.common.utils.CommonUtil;
import io.bicycle.integration.connector.ConfiguredConnectorStream;
import io.bicycle.integration.connector.KnowledgeBaseConnectorResponse;
import io.bicycle.integration.connector.runtime.BackFillConfiguration;
import io.bicycle.integration.connector.runtime.RuntimeConfig;
import io.bicycle.server.event.mapping.config.EventMappingConfigurations;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author sumitmaheshwari
 * Created on 28/05/2022
 */
public abstract class BaseKnowledgeBaseConnector extends BaseConnector implements Source {
    private static final int MAX_RETRY = 3;
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
    private ConnectorConfigManager connectorConfigManager;
    protected BlackListedFields blackListedFields;
    protected static final String TENANT_ID = "tenantId";
    protected String ENV_TENANT_ID_KEY = "TENANT_ID";
    private static final String CONNECTORS_WITH_WAIT_ENABLED = "CONNECTORS_WITH_WAIT_ENABLED";
    private static final String CONNECTORS_WAIT_TIME_IN_MILLIS = "CONNECTORS_WAIT_TIME_IN_MILLIS";
    private static final int MAX_RETRY_COUNT = 3;

    protected List<String> listOfConnectorsWithSleepEnabled = new ArrayList<>();

    protected EventSourceInfo eventSourceInfo;

    protected ObjectMapper objectMapper = new ObjectMapper();
    protected JsonNode config;
    protected ConfiguredAirbyteCatalog catalog;
    protected Map<String, Object> additionalProperties;
    protected JsonNode state;

    protected ConnectionServiceClient connectionServiceClient;
    protected BlobStoreClient blobStoreClient;

    protected CommonUtil commonUtil = new CommonUtil();
    protected BlobStoreBroker blobStoreBroker;
    protected ConnectorConfigService connectorConfigService;

    public RuntimeConfig getRuntimeConfig() {
        return runtimeConfig;
    }

    protected RuntimeConfig runtimeConfig;

    public Logger getLogger() {
        return logger;
    }

    public BaseKnowledgeBaseConnector(SystemAuthenticator systemAuthenticator,
                                      EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier,
                                      ConnectorConfigManager connectorConfigManager) {
        this.systemAuthenticator = systemAuthenticator;
        this.eventConnectorJobStatusNotifier = eventConnectorJobStatusNotifier;
        this.connectorConfigManager = connectorConfigManager;
        String envConnectorsUsingPreviewStore =
                CommonUtil.getFromEnvironment(CONNECTORS_WITH_WAIT_ENABLED, false);
        if (!StringUtils.isEmpty(envConnectorsUsingPreviewStore)) {
            String[] connectorsWithSleepEnabled = envConnectorsUsingPreviewStore.split(",");
            listOfConnectorsWithSleepEnabled = Arrays.asList(connectorsWithSleepEnabled);
        } else {
            listOfConnectorsWithSleepEnabled.add("ad2e5fb0-4218-462c-8f5d-9dc76f5ac9b6");
        }
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


    public EventConnectorJobStatusNotifier getEventConnectorJobStatusNotifier() {
        return eventConnectorJobStatusNotifier;
    }

    abstract protected int getTotalRecordsConsumed();

    public void setBicycleEventProcessorAndPublisher(BicycleConfig bicycleConfig) {
        try {
            this.bicycleConfig = bicycleConfig;
            configStoreClient = getConfigClient(bicycleConfig);
            schemaStoreApiClient = getSchemaStoreApiClient(bicycleConfig);
            entityStoreApiClient = getEntityStoreApiClient(bicycleConfig);

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
        } catch (Throwable e) {
            logger.error("Exception while setting bicycle event process and publisher", e);
        }
    }

    protected ConfiguredConnectorStream getConfiguredConnectorStream(AuthInfo authInfo, String streamId) {
        return connectorConfigService.getConnectorStreamConfigById(authInfo, streamId);
    }

    protected Config getConfigByReference(AuthInfo authInfo, ConfigReference configReference) {
        try {
            return configStoreClient.getLatest(authInfo, configReference);
        } catch (ConfigStoreException e) {
            throw new RuntimeException(e);
        } catch (ConfigNotFoundException e) {
            throw new RuntimeException(e);
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

    private static SchemaStoreApiClient getSchemaStoreApiClient(BicycleConfig bicycleConfig) {
        return new SchemaStoreApiClient(new GenericApiClient(), new ServiceLocator() {
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
        eventConnectorJobStatusNotifier.sendStatus(jobExecutionStatus, message, bicycleConfig.getConnectorId(),
                getTotalRecordsConsumed(), authInfo);
        logger.info(message + " for connector {}", bicycleConfig.getConnectorId());
    }

    public KnowledgeBaseConnectorResponse getKnowledgeBaseConnectorResponse(JsonNode config,
                                                                            ConfiguredAirbyteCatalog catalog) {

        logger.info("Inside knowledge base connector with Config received {} and additional properties {}", config,
                catalog.getAdditionalProperties());

        int retry = 3;
        while (retry > 0) {
            try {
                this.config = config;
                this.catalog = catalog;
                this.additionalProperties = catalog.getAdditionalProperties();
                BicycleConfig bicycleConfig = getBicycleConfig();
                setBicycleEventProcessorAndPublisher(bicycleConfig);
                getConnectionServiceClient();
                blobStoreBroker = new BlobStoreBroker(getBlobStoreClient());
                connectorConfigService = new ConnectorConfigServiceImpl(configStoreClient, schemaStoreApiClient,
                        entityStoreApiClient, null, null,
                        null, systemAuthenticator, blobStoreBroker, null);
                return getKnowledgeBaseConnectorResponseInternal(config, catalog);
            } catch (Throwable e) {
                logger.error(
                        "{}, Error while trying to get knowledgebase connector response, will be retried, retries " +
                                "remaining {}", bicycleConfig != null ? bicycleConfig.getConnectorId() : null, retry,
                        e);
                retry -= 1;
            }
        }

        return null;
    }

    public abstract KnowledgeBaseConnectorResponse getKnowledgeBaseConnectorResponseInternal(JsonNode config,
                                                                                             ConfiguredAirbyteCatalog catalog)
            throws JsonProcessingException;

    public String getTenantId() {
        return this.bicycleConfig.getTenantId();
    }

    public EventSourceInfo getEventSourceInfo() {
        return eventSourceInfo;
    }

    public String getEventSourceType() {
        return additionalProperties.containsKey("bicycleEventSourceType") ?
                additionalProperties.get("bicycleEventSourceType").toString() : CommonUtils.UNKNOWN_EVENT_CONNECTOR;
    }

    protected String getConnectorId() {
        return additionalProperties.containsKey("bicycleConnectorId") ?
                additionalProperties.get("bicycleConnectorId").toString() : "";
    }

    private BicycleConfig getBicycleConfig() {
        String serverURL = getBicycleServerURL();
        String metricStoreURL = additionalProperties.containsKey("bicycleMetricStoreURL") ?
                additionalProperties.get("bicycleMetricStoreURL").toString() : "";
        String token =
                additionalProperties.containsKey("bicycleToken") ? additionalProperties.get("bicycleToken").toString() :
                        "";
        String connectorId = getConnectorId();
        String uniqueIdentifier = UUID.randomUUID().toString();
        String tenantId = additionalProperties.containsKey("bicycleTenantId") ?
                additionalProperties.get("bicycleTenantId").toString() : "tenantId";
        String isOnPrem =
                additionalProperties.containsKey("isOnPrem") ? additionalProperties.get("isOnPrem").toString() :
                        "false";
        // String isOnPrem = additionalProperties.get("isOnPrem").toString();
        boolean isOnPremDeployment = Boolean.parseBoolean(isOnPrem);
        return new BicycleConfig(serverURL, metricStoreURL, token, connectorId, uniqueIdentifier, tenantId,
                systemAuthenticator, isOnPremDeployment);
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

    public AirbyteStateMessage getState(AuthInfo authInfo, String streamId) {

        try {
            String state = connectionServiceClient.getReadStateConfigById(authInfo, streamId);
            if (!StringUtils.isEmpty(state)) {
                AirbyteStateMessage airbyteMessage = objectMapper.readValue(state, AirbyteStateMessage.class);
                return airbyteMessage;
            }
        } catch (Throwable e) {
            logger.error("Unable to get state for streamId " + streamId, e);
        }

        return null;
    }

    protected String getStateAsString(AuthInfo authInfo, String streamId) {

        try {
            String state = connectionServiceClient.getReadStateConfigById(authInfo, streamId);
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
                logger.error("Exception updating the status [{}] [{}] [{}]", getTenantId(), getConnectorId(), payload,
                        e);
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
        String serverURL = additionalProperties.containsKey("bicycleServerURL") ?
                additionalProperties.get("bicycleServerURL").toString() : "";
        String metricStoreURL = additionalProperties.containsKey("bicycleMetricStoreURL") ?
                additionalProperties.get("bicycleMetricStoreURL").toString() : "";
        String token =
                additionalProperties.containsKey("bicycleToken") ? additionalProperties.get("bicycleToken").toString() :
                        "";
        String connectorId = getConnectorId(additionalProperties);
        String uniqueIdentifier = UUID.randomUUID().toString();
        String tenantId = additionalProperties.containsKey("bicycleTenantId") ?
                additionalProperties.get("bicycleTenantId").toString() : "tenantId";
        String isOnPrem = additionalProperties.get("isOnPrem").toString();
        boolean isOnPremDeployment = Boolean.parseBoolean(isOnPrem);
        BicycleConfig bicycleConfig =
                new BicycleConfig(serverURL, metricStoreURL, token, connectorId, uniqueIdentifier, tenantId,
                        systemAuthenticator, isOnPremDeployment);
        runtimeConfig = this.getConnectorConfigManager().getRuntimeConfig(bicycleConfig.getAuthInfo(), connectorId);
        if (runtimeConfig != null && connectorConfigManager.isDefaultConfig(runtimeConfig)) {
            runtimeConfig = null;
        }
        return bicycleConfig;
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
