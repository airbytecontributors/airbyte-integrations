package io.airbyte.integrations.source.event.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.CommonConstants;
import io.airbyte.integrations.bicycle.base.integration.CommonUtils;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.server.event.mapping.UserServiceMappingRule;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sumitmaheshwari
 * Created on 10/10/2023
 */
public class BigQueryEventConnector extends BaseEventConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryEventConnector.class);
    private BigQuerySource bigQuerySource = new BigQuerySource();
    private AtomicBoolean stopConnectorBoolean = new AtomicBoolean(false);
    private final io.bicycle.integration.common.kafka.processing.CommonUtils commonUtils =
            new io.bicycle.integration.common.kafka.processing.CommonUtils();

    public BigQueryEventConnector(SystemAuthenticator systemAuthenticator,
                                  EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier,
                                  ConnectorConfigManager connectorConfigManager) {
        super(systemAuthenticator, eventConnectorJobStatusNotifier, connectorConfigManager);

    }

    @Override
    protected int getTotalRecordsConsumed() {
        return 0;
    }

    @Override
    public void stopEventConnector() {
        stopConnectorBoolean.set(true);
        super.stopEventConnector("Bigquery Event Connector Stopped manually", JobExecutionStatus.success);
    }

    @Override
    public List<RawEvent> convertRecordsToRawEvents(List<?> records) {

        List<RawEvent> rawEvents = new ArrayList<>();
        List<JsonNode> jsonRecords = (List<JsonNode>) records;
        for (JsonNode jsonNode : jsonRecords) {
            try {
                if (jsonNode.isTextual()) {
                    ObjectReader objectReader = objectMapper.reader();
                    jsonNode = objectReader.readTree(jsonNode.textValue());
                }
                ObjectNode objectNode = (ObjectNode) jsonNode;
                objectNode.put(CommonConstants.CONNECTOR_IN_TIMESTAMP, System.currentTimeMillis());
                jsonNode = objectNode;
            } catch (Exception e) {
                LOGGER.error("Error while adding record metadata {}", e);
            }
            JsonRawEvent jsonRawEvent = createJsonRawEvent(jsonNode);
            rawEvents.add(jsonRawEvent);
        }

        if (rawEvents.size() == 0) {
            return null;
        }
        return rawEvents;
    }

    @Override
    public AutoCloseableIterator<AirbyteMessage> preview(JsonNode config, ConfiguredAirbyteCatalog catalog,
                                                         JsonNode state)
            throws InterruptedException, ExecutionException {

        try {
            // BigQuerySource bigQuerySource1 = new BigQuerySource();
            return bigQuerySource.read(config, catalog, state);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AutoCloseableIterator<AirbyteMessage> doRead(JsonNode config, ConfiguredAirbyteCatalog catalog,
                                                        JsonNode state) throws Exception {

        LOGGER.info("======Starting read operation for big query event connector=======");

        Map<String, Object> additionalProperties = catalog.getAdditionalProperties();
        stopConnectorBoolean.set(false);
        String eventSourceType = additionalProperties.containsKey("bicycleEventSourceType") ?
                additionalProperties.get("bicycleEventSourceType").toString() : CommonUtils.UNKNOWN_EVENT_CONNECTOR;
        String connectorId = additionalProperties.containsKey("bicycleConnectorId")
                ? additionalProperties.get("bicycleConnectorId").toString() : "";

        eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);


        //ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);

        LOGGER.info("Inside doRead for connector {} with config {} and catalog {}", connectorId,
                config, catalog);
        AuthInfo authInfo = bicycleConfig.getAuthInfo();

        try {

           /* ElasticMetricsGenerator elasticMetricsGenerator = new ElasticMetricsGenerator(bicycleConfig,
                    eventSourceInfo, config, bicycleEventPublisher, this);
            ses.scheduleAtFixedRate(elasticMetricsGenerator, 60, 30, TimeUnit.SECONDS);*/
            try {
                eventConnectorJobStatusNotifier.sendStatus(JobExecutionStatus.processing,
                        "Big Query Event Connector started Successfully", connectorId, 0, authInfo);
            } catch (Exception e) {
                LOGGER.error("Exception while getting sending big query event connector status");
            }

            LOGGER.info("Trying to get the state for big query with connectorId {}", connectorId);
            AirbyteStateMessage airbyteStateMessage = getState(authInfo, connectorId);
            LOGGER.info("Fetching state {} for big query for connectorId {}", airbyteStateMessage, connectorId);

            String currentState = null;
            if (airbyteStateMessage != null) {
                //TODO: get BigQuery current State
            }
            LOGGER.info("Current state from big query {}", currentState);

            while (!this.getStopConnectorBoolean().get()) {

                //Otherwise authinfo might get expired
                AutoCloseableIterator<AirbyteMessage> iterator = bigQuerySource.read(config, catalog, state);
                List<JsonNode> jsonEvents = new ArrayList<>();
                while (iterator.hasNext()) {
                    AirbyteMessage message = iterator.next();
                    jsonEvents.add(message.getRecord().getData());
                }
                LOGGER.info("Read {} messages for connector Id {}", jsonEvents.size(), connectorId);
                EventProcessorResult eventProcessorResult = null;
                try {
                    List<RawEvent> rawEvents = this.convertRecordsToRawEvents(jsonEvents);
                    List<UserServiceMappingRule> userServiceMappingRules =
                            this.getUserServiceMappingRules(authInfo, eventSourceInfo);
                    if (userServiceMappingRules == null) {
                        continue;
                    }
                    eventProcessorResult = this.convertRawEventsToBicycleEvents(authInfo, eventSourceInfo,
                            rawEvents, userServiceMappingRules);
                } catch (Exception exception) {
                    LOGGER.error("Unable to convert raw records to bicycle events for {} ", connectorId, exception);
                }
                LOGGER.info("Successfully converted messages to raw events for connector Id {}", connectorId);
                try {
                    this.publishEvents(authInfo, eventSourceInfo, eventProcessorResult);

                } catch (Exception exception) {
                    LOGGER.error("Unable to publish bicycle events for {} {} ", connectorId, exception);
                }
                LOGGER.info("Successfully published messages for connector Id {}", connectorId);
                authInfo = bicycleConfig.getAuthInfo();
            }
            LOGGER.info("Shutting down the Big Query Event Connector manually for connector {}",
                    bicycleConfig.getConnectorId());
        } finally {
            //  ses.shutdown();
            LOGGER.info("Closed server connection.");
        }

        return null;
    }

    @Override
    public AirbyteConnectionStatus check(JsonNode config) throws Exception {
        return bigQuerySource.check(config);
    }

    @Override
    public AirbyteCatalog discover(JsonNode config) throws Exception {
        return bigQuerySource.discover(config);
    }

    protected AtomicBoolean getStopConnectorBoolean() {
        return stopConnectorBoolean;
    }

}
