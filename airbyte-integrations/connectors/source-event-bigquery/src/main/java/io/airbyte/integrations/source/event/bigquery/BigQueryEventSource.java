package io.airbyte.integrations.source.event.bigquery;

import static io.bicycle.integration.common.constants.EventConstants.SOURCE_ID;
import ai.apptuit.metrics.client.TagEncodedMetricName;
import ai.apptuit.ml.utils.MetricUtils;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.AbstractIterator;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.CommonConstants;
import io.airbyte.integrations.bicycle.base.integration.CommonUtils;
import io.airbyte.integrations.bicycle.base.integration.DevAuthInfo;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.integrations.source.event.bigquery.data.formatter.DataFormatter;
import io.airbyte.integrations.source.event.bigquery.data.formatter.DataFormatterFactory;
import io.airbyte.integrations.source.event.bigquery.data.formatter.DataFormatterType;
import io.airbyte.integrations.source.relationaldb.models.DbState;
import io.airbyte.integrations.source.relationaldb.models.DbStreamState;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.AirbyteStreamState;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.server.event.mapping.UserServiceMappingRule;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sumitmaheshwari
 * Created on 10/10/2023
 */
public class BigQueryEventSource extends BaseEventConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryEventSource.class);
    public static final TagEncodedMetricName BIGQUERY_CYCLE_TIME = TagEncodedMetricName
            .decode("connector_cycle");

    public static final TagEncodedMetricName BIGQUERY_PULL_RECORDS_TIME = TagEncodedMetricName
            .decode("connector_pull_records");

    public static final TagEncodedMetricName BIGQUERY_PROCESS_RECORDS_TIME = TagEncodedMetricName
            .decode("connector_process_records");

    public static final TagEncodedMetricName BIGQUERY_PUBLISH_RECORDS_TIME = TagEncodedMetricName
            .decode("connector_publish_records");
    private BicycleBigQueryWrapper bicycleBigQueryWrapper = new BicycleBigQueryWrapper();
    private AtomicBoolean stopConnectorBoolean = new AtomicBoolean(false);
    private AtomicLong totalRecordsProcessed = new AtomicLong(0);
    private final io.bicycle.integration.common.kafka.processing.CommonUtils commonUtils =
            new io.bicycle.integration.common.kafka.processing.CommonUtils();

    public BigQueryEventSource(SystemAuthenticator systemAuthenticator,
                               EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier,
                               ConnectorConfigManager connectorConfigManager) {
        super(systemAuthenticator, eventConnectorJobStatusNotifier, connectorConfigManager);

    }

    @Override
    protected int getTotalRecordsConsumed() {
        return totalRecordsProcessed.intValue();
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

            //List<String> streamNames = getStreamNames(catalog.getStreams());

            BigQueryEventSourceConfig bigQueryEventSourceConfig = new BigQueryEventSourceConfig(config,
                    Collections.emptyList(), null);
            bicycleBigQueryWrapper = new BicycleBigQueryWrapper(bigQueryEventSourceConfig);

            AutoCloseableIterator<AirbyteMessage> messagesIterator =
                    bicycleBigQueryWrapper.read(config, catalog, state);

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


        String connectorId = additionalProperties.containsKey("bicycleConnectorId")
                ? additionalProperties.get("bicycleConnectorId").toString() : "";
        DataFormatter dataFormatter = getDataFormatter(config);

        AirbyteCatalog airbyteCatalog = discover(config);
        List<AirbyteStream> allStreams = airbyteCatalog.getStreams();

        BigQueryStreamGetter bigQueryStreamGetter = new BigQueryStreamGetter(connectorId,
                this, config, allStreams);

        handleDataFormatter(connectorId, dataFormatter, catalog, bigQueryStreamGetter);


        Map<String, Object> additionalProperties = catalog.getAdditionalProperties();
        stopConnectorBoolean.set(false);
        String eventSourceType = additionalProperties.containsKey("bicycleEventSourceType") ?
                additionalProperties.get("bicycleEventSourceType").toString() : CommonUtils.UNKNOWN_EVENT_CONNECTOR;


        eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);

        LOGGER.info("Inside doRead for connector {} with config {} and catalog {}", connectorId,
                config, catalog);

        //TODO: Assumption here is that for all streams cursor field is same.
        //TODO: need to handle it.
        String cursorField = getCursorField(catalog, dataFormatter);

        List<String> streamNames = getStreamNames(catalog.getStreams());

        BigQueryEventSourceConfig bigQueryEventSourceConfig = new BigQueryEventSourceConfig(config, streamNames,
                cursorField);

        bicycleBigQueryWrapper = new BicycleBigQueryWrapper(bigQueryEventSourceConfig);
        ScheduledExecutorService ses = Executors.newScheduledThreadPool(3);

        AuthInfo authInfo = bicycleConfig.getAuthInfo();
      /*  if (authInfo == null) {
            authInfo = new DevAuthInfo();
        }*/

        try {

            try {
                BigQueryEventSourceMetricGenerator elasticMetricsGenerator =
                        new BigQueryEventSourceMetricGenerator(bicycleConfig,
                                eventSourceInfo, config, bicycleEventPublisher, this,
                                bigQueryEventSourceConfig);
                ses.scheduleAtFixedRate(elasticMetricsGenerator, 60, 120, TimeUnit.SECONDS);

                if (dataFormatter != null) {
                    ses.scheduleAtFixedRate(bigQueryStreamGetter, 60, 300, TimeUnit.SECONDS);
                }
                LOGGER.info("Successfully started BigQueryEventSourceMetricGenerator and BigQueryStreamGetter");
            } catch (Exception e) {
                LOGGER.error("Unable to start BigQueryEventSourceMetricGenerator or BigQueryStreamGetter", e);
            }

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

                authInfo = bicycleConfig.getAuthInfo();
                handleAtConsumerBegin(connectorId, dataFormatter, catalog, bigQueryStreamGetter);

                Timer.Context consumerCycleTimer = MetricUtils.getMetricRegistry().timer(
                        BIGQUERY_CYCLE_TIME
                                .withTags(SOURCE_ID, bicycleConfig.getConnectorId())
                                .toString()
                ).time();

                //TODO: need to remove
              /*  if (authInfo == null) {
                    authInfo = new DevAuthInfo();
                }*/

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

                Timer.Context getRecordsTimer = MetricUtils.getMetricRegistry().timer(
                        BIGQUERY_PULL_RECORDS_TIME
                                .withTags(SOURCE_ID, bicycleConfig.getConnectorId())
                                .toString()
                ).time();

                AutoCloseableIterator<AirbyteMessage> iterator = bicycleBigQueryWrapper.read(config, catalog, state);

                getRecordsTimer.stop();

                boolean isStateFound = false;

                while (iterator.hasNext()) {
                    AirbyteMessage message = iterator.next();
                    final boolean isState = message.getType() == AirbyteMessage.Type.STATE;
                    if (isState) {
                        AirbyteStateMessage currentState = message.getState();
                        String currentStateAsString = objectMapper.writeValueAsString(currentState.getData());
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
                    handleAtConsumerBegin(connectorId, dataFormatter, catalog, bigQueryStreamGetter);
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
                    Timer.Context processRecordsTimer = MetricUtils.getMetricRegistry().timer(
                            BIGQUERY_PROCESS_RECORDS_TIME
                                    .withTags(SOURCE_ID, bicycleConfig.getConnectorId())
                                    .toString()
                    ).time();
                    List<RawEvent> rawEvents = this.convertRecordsToRawEvents(jsonEvents);
                    eventProcessorResult = this.convertRawEventsToBicycleEvents(authInfo, eventSourceInfo,
                            rawEvents, userServiceMappingRules);
                    processRecordsTimer.stop();
                } catch (Exception exception) {
                    LOGGER.error("Unable to convert raw records to bicycle events for {} ", connectorId, exception);
                    throw exception;
                }
                LOGGER.info("Successfully converted messages to raw events for connector Id {}", connectorId);

                try {
                    Timer.Context publishRecordsTimer = MetricUtils.getMetricRegistry().timer(
                            BIGQUERY_PUBLISH_RECORDS_TIME
                                    .withTags(SOURCE_ID, bicycleConfig.getConnectorId())
                                    .toString()
                    ).time();
                    boolean success = this.publishEvents(authInfo, eventSourceInfo, eventProcessorResult);
                    publishRecordsTimer.stop();
                    if (success) {
                        setStateAsString(authInfo, connectorId, updatedState);
                        state = updatedState;
                        LOGGER.info("Successfully published messages for connector Id {}", connectorId);
                        totalRecordsProcessed.addAndGet(jsonEvents.size());
                    }
                } catch (Exception exception) {
                    LOGGER.error("Unable to publish bicycle events for {} {} ", connectorId, exception);
                    throw exception;
                }
                consumerCycleTimer.stop();
            }
        } finally {
            ses.shutdown();
            LOGGER.info("Closed server connection for big query");
        }

        return null;
    }

    private void handleAtConsumerBegin(String connectorId, DataFormatter dataFormatter,
                                       ConfiguredAirbyteCatalog catalog,
                                       BigQueryStreamGetter bigQueryStreamGetter) {
        //In case of GA streams would come dynamically each day, so need to keep refreshing and update
        //catalog with it.
        handleDataFormatter(connectorId, dataFormatter, catalog, bigQueryStreamGetter);
    }

    private List<String> getStreamNames(List<ConfiguredAirbyteStream> streams) {
        List<String> streamNames = new ArrayList<>();

        for (ConfiguredAirbyteStream configuredAirbyteStream : streams) {
            streamNames.add(configuredAirbyteStream.getStream().getName());
        }

        return streamNames;
    }

    private void handleDataFormatter(String connectorId, DataFormatter dataFormatter, ConfiguredAirbyteCatalog catalog,
                                     BigQueryStreamGetter bigQueryStreamGetter) {

        if (dataFormatter == null || bigQueryStreamGetter == null) {
            return;
        }

        List<AirbyteStream> streams = bigQueryStreamGetter.getStreamList();
        dataFormatter.updateConfiguredAirbyteCatalogWithInterestedStreams(connectorId, catalog,
                streams);

    }

    private String getCursorField(ConfiguredAirbyteCatalog catalog, DataFormatter dataFormatter) {

        if (dataFormatter != null) {
            return dataFormatter.getCursorFieldName();
        }

        List<String> cursorFields = catalog.getStreams().get(0).getCursorField();
        if (cursorFields.size() > 0) {
            return cursorFields.get(0);
        }

        return null;
    }

    @Override
    public AirbyteConnectionStatus check(JsonNode config) throws Exception {
        return bicycleBigQueryWrapper.check(config);
    }

    @Override
    public AirbyteCatalog discover(JsonNode config) throws Exception {
        return bicycleBigQueryWrapper.discover(config);
    }

    protected AtomicBoolean getStopConnectorBoolean() {
        return stopConnectorBoolean;
    }

    private DataFormatter getDataFormatter(JsonNode config) {

        String dataFormatterType = config.has("data_format") ? config.get("data_format").asText() : null;

        try {
            if (!StringUtils.isEmpty(dataFormatterType) && !dataFormatterType.equals("None")) {
                DataFormatter dataFormatter =
                        DataFormatterFactory.getDataFormatter(DataFormatterType.valueOf(dataFormatterType));
                LOGGER.info("Data formatter returned is {}", dataFormatter);
                return dataFormatter;
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

}
