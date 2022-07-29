package io.airbyte.integrations.bicycle.base.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.inception.common.client.ServiceLocator;
import com.inception.common.client.impl.GenericApiClient;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.config.Config;
import com.inception.server.config.ConfigReference;
import com.inception.server.config.api.ConfigNotFoundException;
import com.inception.server.config.api.ConfigStoreException;
import com.inception.server.configstore.client.ConfigStoreAPIClient;
import com.inception.server.configstore.client.ConfigStoreClient;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.Source;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.event.processor.api.BicycleEventProcessor;
import io.bicycle.event.processor.impl.BicycleEventProcessorImpl;
import io.bicycle.event.publisher.api.BicycleEventPublisher;
import io.bicycle.event.publisher.impl.BicycleEventPublisherImpl;
import io.bicycle.server.event.mapping.config.EventMappingConfigurations;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.models.publisher.EventPublisherResult;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sumitmaheshwari
 * Created on 28/05/2022
 */
public abstract class BaseEventConnector extends BaseConnector implements Source {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private BicycleEventProcessor bicycleEventProcessor;
    protected BicycleEventPublisher bicycleEventPublisher;
    protected BicycleConfig bicycleConfig;
    protected SystemAuthenticator systemAuthenticator;
    public static final String STREAM_NAME = "stream_name";
    protected EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier;
    protected static final String TENANT_ID = "tenantId";
    protected String ENV_TENANT_ID_KEY = "TENANT_ID";
    private boolean isOnPremDeployment;
    private String serverURL;
    private String metricStoreURL;
    private String uniqueIdentifier;
    private String token;
    protected String connectorId;
    protected String eventSourceType;
    private String tenantId;
    private String isOnPrem;
    protected EventSourceInfo eventSourceInfo;

    public BaseEventConnector(SystemAuthenticator systemAuthenticator, EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier) {
        this.systemAuthenticator = systemAuthenticator;
        this.eventConnectorJobStatusNotifier = eventConnectorJobStatusNotifier;
    }

    public void createBicycleConfigFromConfigAndCatalog (final JsonNode config, final ConfiguredAirbyteCatalog catalog, final JsonNode state) {
        Map<String, Object> additionalProperties = catalog.getAdditionalProperties();
        serverURL = additionalProperties.containsKey("bicycleServerURL") ? additionalProperties.get("bicycleServerURL").toString() : "";
        metricStoreURL = additionalProperties.containsKey("bicycleMetricStoreURL") ? additionalProperties.get("bicycleMetricStoreURL").toString() : "";
        uniqueIdentifier = UUID.randomUUID().toString();
        token = additionalProperties.containsKey("bicycleToken") ? additionalProperties.get("bicycleToken").toString() : "";
        connectorId = additionalProperties.containsKey("bicycleConnectorId") ? additionalProperties.get("bicycleConnectorId").toString() : "";
        eventSourceType = additionalProperties.containsKey("bicycleEventSourceType") ? additionalProperties.get("bicycleEventSourceType").toString() : "EVENT";
        tenantId = additionalProperties.containsKey("bicycleTenantId") ? additionalProperties.get("bicycleTenantId").toString() : "tenantId";;
        isOnPrem = additionalProperties.get("isOnPrem").toString();
        isOnPremDeployment = Boolean.parseBoolean(isOnPrem);
        this.bicycleConfig = new BicycleConfig(serverURL, metricStoreURL,token, connectorId, uniqueIdentifier, tenantId, systemAuthenticator, isOnPremDeployment);
        setBicycleEventProcessor(bicycleConfig);
        eventSourceInfo = new EventSourceInfo(connectorId, eventSourceType);
    }

    private void setBicycleEventProcessor(BicycleConfig bicycleConfig) {
        ConfigStoreClient configStoreClient = getConfigClient(bicycleConfig);
        this.bicycleEventProcessor = new BicycleEventProcessorImpl(configStoreClient);
        EventMappingConfigurations eventMappingConfigurations = new EventMappingConfigurations(bicycleConfig.getServerURL(),bicycleConfig.getMetricStoreURL(), bicycleConfig.getServerURL(),
                bicycleConfig.getEventURL(), bicycleConfig.getServerURL(), bicycleConfig.getEventURL());
        this.bicycleEventPublisher = new BicycleEventPublisherImpl(eventMappingConfigurations, systemAuthenticator, true);
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

    public abstract List<RawEvent> convertRecordsToRawEvents(List<?> records);

    public abstract AutoCloseableIterator<AirbyteMessage> preview(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state);

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

}
