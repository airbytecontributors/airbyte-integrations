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
import io.bicycle.server.event.mapping.models.converter.BicycleEventsResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sumitmaheshwari
 * Created on 28/05/2022
 */
public abstract class BaseEventConnector extends BaseConnector implements Source {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private BicycleEventProcessor bicycleEventProcessor;
    private BicycleEventPublisher bicycleEventPublisher;
    private BicycleConfig bicycleConfig;
    protected SystemAuthenticator systemAuthenticator;
    protected EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier;
    protected static final String TENANT_ID = "tenantId";
    protected String ENV_TENANT_ID_KEY = "TENANT_ID";
    public BaseEventConnector(SystemAuthenticator systemAuthenticator, EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier) {
        this.systemAuthenticator = systemAuthenticator;
        this.eventConnectorJobStatusNotifier = eventConnectorJobStatusNotifier;
    }

    public void setBicycleEventProcessor(BicycleConfig bicycleConfig) {
        this.bicycleConfig = bicycleConfig;
        ConfigStoreClient configStoreClient = getConfigClient(bicycleConfig);
        this.bicycleEventProcessor = new BicycleEventProcessorImpl(configStoreClient);
        EventMappingConfigurations eventMappingConfigurations = new EventMappingConfigurations
                (bicycleConfig.getServerURL(), bicycleConfig.getServerURL(), bicycleConfig.getServerURL(),
                        bicycleConfig.getEventURL(), bicycleConfig.getServerURL(), bicycleConfig.getEventURL());
        this.bicycleEventPublisher = new BicycleEventPublisherImpl(eventMappingConfigurations, true);
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

    public BicycleEventsResult convertRawEventsToBicycleEvents(AuthInfo authInfo,
                                                               EventSourceInfo eventSourceInfo,
                                                               List<RawEvent> rawEvents) {

        BicycleEventsResult bicycleEventsResult =
                bicycleEventProcessor.processEventsForPipeline(authInfo, eventSourceInfo, rawEvents);

        return bicycleEventsResult;

    }

    public boolean publishEvents(AuthInfo authInfo, EventSourceInfo eventSourceInfo,
                                 BicycleEventsResult bicycleEventsResult) {

        if (bicycleEventsResult == null) {
            return true;
        }
        boolean published = bicycleEventPublisher.publishEvents(authInfo, eventSourceInfo, bicycleEventsResult);

        if (!published) {
            logger.warn("There was some issue in publishing events");
        }

        return published;
    }

}
