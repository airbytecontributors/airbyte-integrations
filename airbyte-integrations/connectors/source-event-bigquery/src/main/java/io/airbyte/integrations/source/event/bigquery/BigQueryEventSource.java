package io.airbyte.integrations.source.event.bigquery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.AbstractIterator;
import com.inception.server.auth.api.AuthorizeUser;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.auth.model.AuthType;
import com.inception.server.auth.model.BearerTokenAuthInfo;
import com.inception.server.auth.model.Principal;
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.CommonConstants;
import io.airbyte.integrations.bicycle.base.integration.CommonUtils;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.integrations.source.event.bigquery.data.formatter.DataFormatter;
import io.airbyte.integrations.source.event.bigquery.data.formatter.DataFormatterFactory;
import io.airbyte.integrations.source.event.bigquery.data.formatter.DataFormatterType;
import io.airbyte.integrations.source.relationaldb.models.DbState;
import io.airbyte.integrations.source.relationaldb.models.DbStreamState;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStreamState;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.SyncMode;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.server.event.mapping.UserServiceMappingRule;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sumitmaheshwari
 * Created on 10/10/2023
 */
public class BigQueryEventSource extends BaseEventConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryEventSource.class);
    private BigQuerySource bigQuerySource = new BigQuerySource();
    private AtomicBoolean stopConnectorBoolean = new AtomicBoolean(false);
    private final io.bicycle.integration.common.kafka.processing.CommonUtils commonUtils =
            new io.bicycle.integration.common.kafka.processing.CommonUtils();

    public BigQueryEventSource(SystemAuthenticator systemAuthenticator,
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

        return rawEvents;
    }

    @Override
    public AutoCloseableIterator<AirbyteMessage> preview(JsonNode config, ConfiguredAirbyteCatalog catalog,
                                                         JsonNode state)
            throws InterruptedException, ExecutionException {

        try {
            DataFormatter dataFormatter = getDataFormatter(config);
            LOGGER.info("State information for preview {}", state);
            if (dataFormatter != null && state != null) {
                dataFormatter.updateSyncMode(catalog);
                LOGGER.info("Updated sync mode for preview");
            }

            AutoCloseableIterator<AirbyteMessage> messagesIterator = bigQuerySource.read(config, catalog, state);

            if (dataFormatter != null) {
                return AutoCloseableIterators.fromIterator(new AbstractIterator<>() {
                    @Override
                    protected AirbyteMessage computeNext() {
                        if (messagesIterator.hasNext()) {
                            AirbyteMessage message = messagesIterator.next();
                            if (message.getType().equals(AirbyteMessage.Type.RECORD) && message.getRecord() != null) {
                                JsonNode jsonNode = message.getRecord().getData();
                                jsonNode = dataFormatter.formatEvent(jsonNode);
                                message.getRecord().setData(jsonNode);
                            }
                            return message;
                        }
                        return endOfData();
                    }
                });
            } else {
                return messagesIterator;
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AutoCloseableIterator<AirbyteMessage> doRead(JsonNode config, ConfiguredAirbyteCatalog catalog,
                                                        JsonNode state) throws Exception {


        Map<String, Object> additionalProperties = catalog.getAdditionalProperties();
        stopConnectorBoolean.set(false);
        String eventSourceType = additionalProperties.containsKey("bicycleEventSourceType") ?
                additionalProperties.get("bicycleEventSourceType").toString() : CommonUtils.UNKNOWN_EVENT_CONNECTOR;
        String connectorId = additionalProperties.containsKey("bicycleConnectorId")
                ? additionalProperties.get("bicycleConnectorId").toString() : "";

        eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);

        LOGGER.info("Inside doRead for connector {} with config {} and catalog {}", connectorId,
                config, catalog);

        DataFormatter dataFormatter = getDataFormatter(config);
        //TODO: this is workaround until we expose the option to set sync mode in UI
        if (dataFormatter != null) {
            dataFormatter.updateSyncMode(catalog);
        }

        //ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);

        AuthInfo authInfo = bicycleConfig.getAuthInfo();
    /*    if (authInfo == null) {
            authInfo = new DevAuthInfo();
        }*/

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
            String savedState = getStateAsString(authInfo, connectorId);
            LOGGER.info("Fetching state {} for big query for connectorId {}", savedState, connectorId);

            if (!StringUtils.isEmpty(savedState)) {
                state = Jsons.deserialize(savedState);
            }
            LOGGER.info("Current state from big query {}", state);
            JsonNode updatedState = null;

            while (!this.getStopConnectorBoolean().get()) {

                //TODO: need to remove
            /*    if (authInfo == null) {
                    authInfo = new DevAuthInfo();
                }
*/
                List<JsonNode> jsonEvents = new ArrayList<>();
                List<UserServiceMappingRule> userServiceMappingRules =
                        this.getUserServiceMappingRules(authInfo, eventSourceInfo);

                //if mapping rules are returned null, means there was a problem in downloading rules.
                //so no point in trying to fetch the records to read
                if (userServiceMappingRules == null) {
                    continue;
                }
                LOGGER.info("Successfully downloaded the rules with size {} for connector {}",
                        userServiceMappingRules.size(), connectorId);
                AutoCloseableIterator<AirbyteMessage> iterator = bigQuerySource.read(config, catalog, state);
                boolean isStateFound = false;

                while (iterator.hasNext()) {
                    AirbyteMessage message = iterator.next();
                    final boolean isState = message.getType() == AirbyteMessage.Type.STATE;
                    if (isState) {
                        AirbyteStateMessage currentState = message.getState();
                        String currentStateAsString = objectMapper.writeValueAsString(currentState);
                        LOGGER.info("Found state message {}", currentStateAsString);
                        updatedState = Jsons.deserialize(currentStateAsString);
                        isStateFound = true;
                        continue;
                    }
                    if (message.getRecord() != null) {
                        JsonNode jsonNode = message.getRecord().getData();
                        if (dataFormatter != null) {
                            jsonNode = dataFormatter.formatEvent(jsonNode);
                        }
                        jsonEvents.add(jsonNode);
                    } else {
                        LOGGER.warn("Message is not of type record but {}", message.getType());
                    }
                }

                LOGGER.info("Read {} messages for connector Id {}", jsonEvents.size(), connectorId);
                if (jsonEvents.size() == 0) {
                    continue;
                }

                if (!isStateFound && dataFormatter != null) {
                    AirbyteStateMessage currentState = createStateMessage(catalog, dataFormatter.getCursorFieldName(),
                            dataFormatter.getCursorFieldValue(jsonEvents));
                    String currentStateAsString = objectMapper.writeValueAsString(currentState.getData());
                    LOGGER.info("Found state message from formatter {}", currentStateAsString);
                    updatedState = Jsons.deserialize(currentStateAsString);
                }

                EventProcessorResult eventProcessorResult = null;

                try {
                    List<RawEvent> rawEvents = this.convertRecordsToRawEvents(jsonEvents);
                    eventProcessorResult = this.convertRawEventsToBicycleEvents(authInfo, eventSourceInfo,
                            rawEvents, userServiceMappingRules);
                } catch (Exception exception) {
                    LOGGER.error("Unable to convert raw records to bicycle events for {} ", connectorId, exception);
                    throw exception;
                }
                LOGGER.info("Successfully converted messages to raw events for connector Id {}", connectorId);

                try {
                    boolean success = this.publishEvents(authInfo, eventSourceInfo, eventProcessorResult);
                    if (success) {
                        setStateAsString(authInfo, connectorId, updatedState);
                        state = updatedState;
                        LOGGER.info("Successfully published messages for connector Id {}", connectorId);
                    }
                } catch (Exception exception) {
                    LOGGER.error("Unable to publish bicycle events for {} {} ", connectorId, exception);
                    throw exception;
                }

                authInfo = bicycleConfig.getAuthInfo();
            }
        } finally {
            //  ses.shutdown();
            LOGGER.info("Closed server connection for big query");
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

    private DataFormatter getDataFormatter(JsonNode config) {

        String dataFormatterType = config.has("data_format") ? config.get("data_format").asText() : null;

        try {
            if (!StringUtils.isEmpty(dataFormatterType) && !dataFormatterType.equals("None")) {
                return DataFormatterFactory.getDataFormatter(DataFormatterType.valueOf(dataFormatterType));
            }
        } catch (Exception e) {
            LOGGER.error("Unable to initialize data formatter for dataformatter type {} {}", dataFormatterType, e);
        }
        return null;
    }

    private AirbyteStateMessage createStateMessage(ConfiguredAirbyteCatalog catalog, String cursorField,
                                                   String cursorFieldValue) {

        DbState dbState = new DbState();
        dbState.setCdc(false);
        DbStreamState dbStreamState = new DbStreamState();

        List<String> cursorFields = new ArrayList<>();
        cursorFields.add(cursorField);

        AirbyteStreamState airbyteStreamState = new AirbyteStreamState();
        ConfiguredAirbyteStream stream = catalog.getStreams().get(0);
        airbyteStreamState.setName(stream.getStream().getName());

        dbStreamState.setStreamName(stream.getStream().getName());
        dbStreamState.setStreamNamespace(stream.getStream().getNamespace());
        dbStreamState.setCursorField(cursorFields);
        dbStreamState.setCursor(cursorFieldValue);

        dbState.getStreams().add(dbStreamState);

        return new AirbyteStateMessage().withData(Jsons.jsonNode(dbState));
    }

    private static class DevAuthInfo implements AuthInfo {

        @Override
        public Principal getPrincipal() {
            return null;
        }

        @Override
        public String getToken() {
            return "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJST0xFIjoiQVBJIiwic3ViIjoiZXZlbnQtZGVtby1hcHAtMiIsIk9SR19JRCI6IjgwIiwiaXNzIjoiamF5YUBiaWN5Y2xlLmlvIiwiaWF0IjoxNjYzNTgyNjgwLCJURU5BTlQiOiJldnQtZmJiOTY3YWQtMjVmMi00ZWVlLWIyZTUtZjUyYjA0N2JlMmVmIiwianRpIjoiZTQxMDhhNDMtYjVmNC00ZmRkLTg5NiJ9.wC6lMnpMvlNMvvyI_TPP4vzHRgPQstu0IUSpkD5aIPg";
        }

        @Override
        public String getTenantCode() {
            return null;
        }

        @Override
        public String getTenantId() {
            return "evt-fbb967ad-25f2-4eee-b2e5-f52b047be2ef";
        }

        @Override
        public String getAccountId() {
            return null;
        }

        @Override
        public AuthType getAuthType() {
            return AuthType.BEARER;
        }

        @Override
        public String[] getRoles() {
            return new String[] {AuthorizeUser.READ_WRITE};
        }
    }

}
