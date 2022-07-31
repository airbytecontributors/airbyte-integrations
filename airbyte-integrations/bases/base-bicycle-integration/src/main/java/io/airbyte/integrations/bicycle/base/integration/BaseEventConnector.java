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
import io.airbyte.protocol.models.AirbyteConnectionStatus;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

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
    private BicycleConfig bicycleConfig;
    protected SystemAuthenticator systemAuthenticator;
    protected EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier;
    protected CountDownLatch shouldStop = new CountDownLatch(1);
    protected static final String TENANT_ID = "tenantId";
    protected String ENV_TENANT_ID_KEY = "TENANT_ID";
    public BaseEventConnector(SystemAuthenticator systemAuthenticator, EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier) {
        this.systemAuthenticator = systemAuthenticator;
        this.eventConnectorJobStatusNotifier = eventConnectorJobStatusNotifier;
    }

    public CountDownLatch shouldStop() {
        return shouldStop;
    }

    public void setBicycleEventProcessor(BicycleConfig bicycleConfig) {
        this.bicycleConfig = bicycleConfig;
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

    public AirbyteConnectionStatus stopEventConnector() {
        shouldStop.countDown();
        return new AirbyteConnectionStatus()
                .withStatus(AirbyteConnectionStatus.Status.SUCCEEDED).withMessage("Stopped Event Connector");
    }

    public abstract List<RawEvent> convertRecordsToRawEvents(List<?> records);

    public abstract AutoCloseableIterator<AirbyteMessage> preview(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) throws InterruptedException, ExecutionException;

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
