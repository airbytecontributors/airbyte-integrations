package io.airbyte.integrations.bicycle.base.integration;

import bicycle.io.events.proto.BicycleEventList;
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
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.Source;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.event.processor.ConfigHelper;
import io.bicycle.event.processor.api.BicycleEventProcessor;
import io.bicycle.event.processor.impl.BicycleEventProcessorImpl;
import io.bicycle.event.publisher.api.BicycleEventPublisher;
import io.bicycle.event.publisher.impl.BicycleEventPublisherImpl;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.integration.common.writer.Writer;
import io.bicycle.integration.connector.ProcessRawEventsResult;
import io.bicycle.integration.connector.ProcessedEventSourceData;
import io.bicycle.integration.connector.SyncDataRequest;
import io.bicycle.server.event.mapping.UserServiceMappingRule;
import io.bicycle.server.event.mapping.config.EventMappingConfigurations;
import io.bicycle.server.event.mapping.models.converter.BicycleEventsResult;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.models.publisher.EventPublisherResult;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sumitmaheshwari
 * Created on 28/05/2022
 */
public abstract class BaseEventConnector extends BaseConnector implements Source {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private final ConfigHelper configHelper = new ConfigHelper();
    private ConfigStoreClient configStoreClient;
    private BicycleEventProcessor bicycleEventProcessor;
    protected BicycleEventPublisher bicycleEventPublisher;
    private BicycleConfig bicycleConfig;
    protected SystemAuthenticator systemAuthenticator;
    protected EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier;
    protected static final String TENANT_ID = "tenantId";
    protected String ENV_TENANT_ID_KEY = "TENANT_ID";
    protected EventSourceInfo eventSourceInfo;
    public BaseEventConnector(SystemAuthenticator systemAuthenticator, EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier) {
        this.systemAuthenticator = systemAuthenticator;
        this.eventConnectorJobStatusNotifier = eventConnectorJobStatusNotifier;
    }

    public EventConnectorJobStatusNotifier getEventConnectorJobStatusNotifier() {
        return eventConnectorJobStatusNotifier;
    }

    abstract protected int getTotalRecordsConsumed();

    public void setBicycleEventProcessorAndPublisher(BicycleConfig bicycleConfig) {
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

    public abstract AutoCloseableIterator<AirbyteMessage> syncData(JsonNode sourceConfig,
                                                                   ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                                                                   JsonNode readState,
                                                                   SyncDataRequest syncDataRequest);

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
                               List<RawEvent> rawEvents) {
        try {
            ProcessRawEventsResult processedEvents = this.processRawEvents(authInfo, eventSourceInfo, rawEvents);
            try {
                writer.writeEventData(
                        configuredConnectorStreamId, readTimestamp, processedEvents.getProcessedEventSourceDataList());
                savePreviewEvents(authInfo, traceInfo, eventSourceInfo, processedEvents);
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
}
