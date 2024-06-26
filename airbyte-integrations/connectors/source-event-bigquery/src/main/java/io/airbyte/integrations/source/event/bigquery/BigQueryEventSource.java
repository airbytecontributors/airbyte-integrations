package io.airbyte.integrations.source.event.bigquery;

import static io.airbyte.integrations.bicycle.base.integration.MetricAsEventsGenerator.SOURCE_TYPE;
import static io.bicycle.integration.common.constants.EventConstants.SOURCE_ID;
import ai.apptuit.metrics.client.TagEncodedMetricName;
import ai.apptuit.ml.utils.MetricUtils;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.AbstractIterator;
import com.google.protobuf.util.JsonFormat;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.CommonConstants;
import io.airbyte.integrations.bicycle.base.integration.CommonUtils;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.integrations.source.event.bigquery.data.formatter.DataFormatter;
import io.airbyte.integrations.source.event.bigquery.data.formatter.DataFormatterConfig;
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
import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.integration.connector.runtime.BackFillConfiguration;
import io.bicycle.integration.connector.runtime.RuntimeConfig;
import io.bicycle.server.event.mapping.UserServiceMappingRule;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
    public static final String STREAM_NAME_TAG = "streamName";
    private BicycleBigQueryWrapper bicycleBigQueryWrapper = new BicycleBigQueryWrapper();

    private AtomicBoolean stopConnectorBoolean = new AtomicBoolean(false);
    private AtomicLong totalRecordsProcessed = new AtomicLong(0);
    private BigQueryEventSourceMetricGenerator bigQueryEventSourceMetricGenerator;

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
    public List<RawEvent> convertRecordsToRawEventsInternal(List<?> records) {

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
            Boolean isInitializedSuccessfully = false;
            try {
                initialize(config, catalog);
                isInitializedSuccessfully = true;
            } catch (Exception e) {
                LOGGER.error("Unable to initialize during preview with catalog {} and config {}", catalog, config);
            }


            BigQueryEventSourceConfig bigQueryEventSourceConfig = new BigQueryEventSourceConfig(config, null);

            DataFormatter dataFormatter = bigQueryEventSourceConfig.getDataFormatter();

            LOGGER.info("State information for preview {}", state);

           /* if (dataFormatter != null && state != null) {
                dataFormatter.updateSyncMode(catalog);
                LOGGER.info("Updated sync mode for preview");
            }*/

            //List<String> streamNames = getStreamNames(catalog.getStreams());

            bicycleBigQueryWrapper = new BicycleBigQueryWrapper(bigQueryEventSourceConfig);

            AutoCloseableIterator<AirbyteMessage> messagesIterator =
                    bicycleBigQueryWrapper.read(config, catalog, state);

            if (dataFormatter != null) {
                Boolean finalIsInitializedSuccessfully = isInitializedSuccessfully;
                return AutoCloseableIterators.fromIterator(new AbstractIterator<>() {
                    @Override
                    protected AirbyteMessage computeNext() {
                        if (messagesIterator.hasNext()) {
                            AirbyteMessage message = messagesIterator.next();
                            if (message.getType().equals(AirbyteMessage.Type.RECORD) && message.getRecord() != null) {
                                JsonNode jsonNode = message.getRecord().getData();
                                List<JsonNode> jsonNodes = new ArrayList<>();
                                if (finalIsInitializedSuccessfully) {
                                   jsonNodes = splitEvents(jsonNode, bicycleConfig.getAuthInfo(),
                                            getConnectorId());
                                } else {
                                    jsonNodes.add(jsonNode);
                                }
                                JsonNode node = jsonNodes.get(0);
                                node = dataFormatter.formatEvent(node);
                                message.getRecord().setData(node);
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

        AuthInfo authInfo = bicycleConfig.getAuthInfo();
        String cursorField = getCursorFieldName(catalog);
        runtimeConfig = connectorConfigManager != null ? connectorConfigManager
                .getRuntimeConfig(authInfo, connectorId) : RuntimeConfig.getDefaultInstance();

        BackFillConfiguration backFillConfiguration = runtimeConfig.getBackFillConfig();

        BigQueryEventSourceConfig bigQueryEventSourceConfig = new BigQueryEventSourceConfig(config, cursorField,
                backFillConfiguration.getEnableBackFill());

        LOGGER.info("BackfillConfig {}", JsonFormat.printer().print(backFillConfiguration));

        DataFormatter dataFormatter = bigQueryEventSourceConfig.getDataFormatter();

        //DataFormatter dataFormatter = getDataFormatter(config);

        //In general we use the streams that are passed in catalog. But in case of big query for GA
        //the stream name keeps changing everyday as new stream with date is created everyday.
        //We have a dataformatter which can be defined for any DataFormatterType and that can handle
        //things specifically to that dataformatter.
        //For ex: in case of GA the dataformatter filters the streams and returns only streams that are required.


        AirbyteCatalog airbyteCatalog = discover(config);
        List<AirbyteStream> allStreams = airbyteCatalog.getStreams();
        long totalRecords = 0;
        int noEventsReceivedCounter = 0;
        if (backFillConfiguration.getEnableBackFill()) {
            totalRecords = getTotalRecords(allStreams, bigQueryEventSourceConfig);
            LOGGER.info("Total records for streams are {}", totalRecords);
            //updateTotalRecordsInReadState(totalRecords);
        }

        //BigQueryEventSourceConfig bigQueryEventSourceConfig = new BigQueryEventSourceConfig(config, cursorField);

        BigQueryStreamGetter bigQueryStreamGetter = new BigQueryStreamGetter(connectorId,
                this, config, allStreams);


        handleDataFormatter(connectorId, dataFormatter, catalog, bigQueryStreamGetter, bigQueryEventSourceConfig);


        Map<String, Object> additionalProperties = catalog.getAdditionalProperties();
        stopConnectorBoolean.set(false);
        String eventSourceType = additionalProperties.containsKey("bicycleEventSourceType") ?
                additionalProperties.get("bicycleEventSourceType").toString() : CommonUtils.UNKNOWN_EVENT_CONNECTOR;


        eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);

        LOGGER.info("Inside doRead for connector {} with config {} and catalog {}", connectorId,
                config, catalog);

        //TODO: Assumption here is that for all streams cursor field is same.
        //TODO: need to handle it.


        bicycleBigQueryWrapper = new BicycleBigQueryWrapper(bigQueryEventSourceConfig);
        ScheduledExecutorService ses = Executors.newScheduledThreadPool(3);


      /*  if (authInfo == null) {
            authInfo = new DevAuthInfo();
        }*/

        try {

            try {

                if (bigQueryEventSourceMetricGenerator == null) {
                    bigQueryEventSourceMetricGenerator =
                            new BigQueryEventSourceMetricGenerator(bicycleConfig,
                                    eventSourceInfo, config, bicycleEventPublisher, this,
                                    bigQueryEventSourceConfig, bigQueryStreamGetter);
                }

                ses.scheduleAtFixedRate(bigQueryEventSourceMetricGenerator, 60, 120, TimeUnit.SECONDS);

                //Periodically run the bigQueryStreamGetter to refresh the new streams
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
            totalRecords = 0;
            long totalRecordsWithoutSplitOverall = 0;
            long totalRecordsWithoutSplit = 0;
            while (!this.getStopConnectorBoolean().get()) {

                authInfo = bicycleConfig.getAuthInfo();
                handleAtConsumerBegin(connectorId, dataFormatter, catalog, bigQueryStreamGetter,
                        bigQueryEventSourceConfig);

                Timer.Context consumerCycleTimer = MetricUtils.getMetricRegistry().timer(
                        BIGQUERY_CYCLE_TIME
                                .withTags(SOURCE_ID, bicycleConfig.getConnectorId())
                                .toString()
                ).time();

                //TODO: need to remove
               /* if (authInfo == null) {
                    authInfo = new DevAuthInfo();
                }*/

                List<JsonNode> jsonEvents = new ArrayList<>();
                totalRecordsWithoutSplit = 0;
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
                        CommonConstants.CONNECTOR_RECORDS_PULL_METRIC
                                .withTags(SOURCE_ID, bicycleConfig.getConnectorId())
                                .withTags(SOURCE_TYPE, eventSourceInfo.getEventSourceType())
                                .toString()
                ).time();

                long startTime = System.currentTimeMillis();
                AutoCloseableIterator<AirbyteMessage> iterator = bicycleBigQueryWrapper.read(config, catalog, state);
                LOGGER.info("Debug:: Done with iterator in {}", System.currentTimeMillis() - startTime);
                getRecordsTimer.stop();

                boolean isStateFound = false;

                getRecordsTimer = MetricUtils.getMetricRegistry().timer(
                        CommonConstants.CONNECTOR_RECORDS_ITERATE_METRIC
                                .withTags(SOURCE_ID, bicycleConfig.getConnectorId())
                                .withTags(SOURCE_TYPE, eventSourceInfo.getEventSourceType())
                                .toString()
                ).time();
                startTime = System.currentTimeMillis();
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
                        totalRecordsWithoutSplit++;
                        totalRecordsWithoutSplitOverall++;
                        JsonNode jsonNode = message.getRecord().getData();
                        List<JsonNode> outputEvents = new ArrayList<>();
                        outputEvents = splitEvents(jsonNode, authInfo, connectorId);
                        for (JsonNode node: outputEvents) {
                            if (dataFormatter != null) {
                                dataFormatter.formatEvent(node);
                            }
                        }
                        jsonEvents.addAll(outputEvents);
                    } else {
                        LOGGER.warn("Message is not of type record but {}", message.getType());
                    }
                }

                getRecordsTimer.stop();

                LOGGER.info("Debug:: Read {} messages for connector Id {} and time taken {}", jsonEvents.size(),
                        connectorId, System.currentTimeMillis() - startTime);
                LOGGER.info("Debug:: Read {} messages without split in this iteration and time taken {} " +
                                "and total overall so far without split are {}", totalRecordsWithoutSplit,
                        System.currentTimeMillis() - startTime, totalRecordsWithoutSplitOverall);
                if (jsonEvents.size() == 0) {
                    noEventsReceivedCounter++;
                    //In case of backfill if we stop receiving events means we are done reading everything
                    if (noEventsReceivedCounter == 3 && backFillConfiguration.getEnableBackFill()) {
                        updateTotalRecordsInReadState(connectorId, totalRecords);
                        stopEventConnector();
                        break;
                    }
                    handleAtConsumerBegin(connectorId, dataFormatter, catalog, bigQueryStreamGetter,
                            bigQueryEventSourceConfig);
                    continue;
                }

                noEventsReceivedCounter = 0;

                if (!isStateFound && dataFormatter != null && StringUtils.isNotEmpty(dataFormatter.getCursorFieldName())) {
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
                LOGGER.info("Debug:: Successfully converted messages to raw events for connector Id {}", connectorId);

                try {
                    Timer.Context publishRecordsTimer = MetricUtils.getMetricRegistry().timer(
                            BIGQUERY_PUBLISH_RECORDS_TIME
                                    .withTags(SOURCE_ID, bicycleConfig.getConnectorId())
                                    .toString()
                    ).time();

                    boolean success = this.publishEvents(authInfo, eventSourceInfo, eventProcessorResult);
                    publishRecordsTimer.stop();
                    if (success) {
                        totalRecords += eventProcessorResult.getUserServiceDefs().size();
                        setStateAsString(authInfo, connectorId, updatedState);
                        if (dataFormatter != null) {
                            String airbyteMessageAsString = objectMapper.writeValueAsString(updatedState);
                            dataFormatter.publishLagMetrics(eventSourceInfo, airbyteMessageAsString);
                        }
                        state = updatedState;
                        totalRecordsProcessed.addAndGet(jsonEvents.size());
                        LOGGER.info("Debug:: Successfully read messages for connector Id {} are {} and published " +
                                        "so far {}", connectorId, totalRecordsProcessed.get(), totalRecords);
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


    protected long getTotalRecords(List<AirbyteStream> streams, BigQueryEventSourceConfig bigQueryEventSourceConfig)
            throws IOException, InterruptedException {
        try {
            String projectId = bigQueryEventSourceConfig.getProjectId();
            String datasetName = bigQueryEventSourceConfig.getDatasetId();
            ServiceAccountCredentials credentials = ServiceAccountCredentials
                    .fromStream(new ByteArrayInputStream
                            (bigQueryEventSourceConfig.getCredentialsJson().getBytes(StandardCharsets.UTF_8)));

            BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId)
                    .setCredentials(credentials)
                    .build().getService();
            long count = 0;
            for (AirbyteStream stream : streams) {
                String tableName = stream.getName();
                String query = "SELECT count(*) AS count FROM `"
                        + projectId + "." + datasetName + "." + tableName + "`";
                QueryJobConfiguration.Builder queryConfigBuilder = QueryJobConfiguration.newBuilder(query)
                        .setPriority(QueryJobConfiguration.Priority.BATCH)
                        .setDefaultDataset(datasetName);

                // Run the query
                TableResult result = bigquery.query(queryConfigBuilder.build());

                for (FieldValueList row : result.iterateAll()) {
                    count += row.get("count").getLongValue();
                }

            }
            return count;
        } catch (Exception e) {
            throw e;
        }
    }

    private void handleAtConsumerBegin(String connectorId, DataFormatter dataFormatter,
                                       ConfiguredAirbyteCatalog catalog,
                                       BigQueryStreamGetter bigQueryStreamGetter,
                                       BigQueryEventSourceConfig bigQueryEventSourceConfig) {
        //In case of GA streams would come dynamically each day, so need to keep refreshing and update
        //catalog with it.
        handleDataFormatter(connectorId, dataFormatter, catalog, bigQueryStreamGetter, bigQueryEventSourceConfig);
    }

    private void handleDataFormatter(String connectorId, DataFormatter dataFormatter, ConfiguredAirbyteCatalog catalog,
                                     BigQueryStreamGetter bigQueryStreamGetter,
                                     BigQueryEventSourceConfig bigQueryEventSourceConfig) {

        if (dataFormatter == null || bigQueryStreamGetter == null) {
            return;
        }

        List<AirbyteStream> streams = bigQueryStreamGetter.getStreamList();
        dataFormatter.updateConfiguredAirbyteCatalogWithInterestedStreams(connectorId, catalog,
                streams, bigQueryEventSourceConfig);

    }

    private String getCursorFieldName(ConfiguredAirbyteCatalog catalog) {

        List<String> cursorFields = catalog.getStreams().get(0).getCursorField();
        if (cursorFields.size() > 0) {
            return cursorFields.get(0);
        }

        return "event_timestamp";
    }

    @Override
    public AirbyteConnectionStatus check(JsonNode config) throws Exception {
        return bicycleBigQueryWrapper.check(config);
    }

    @Override
    public AirbyteCatalog discover(JsonNode config) throws Exception {
        AirbyteCatalog airbyteCatalog = bicycleBigQueryWrapper.discover(config);

        BigQueryEventSourceConfig bigQueryEventSourceConfig = new BigQueryEventSourceConfig(config, null);
        DataFormatterConfig dataFormatterConfig = bigQueryEventSourceConfig.getDataFormatter().getDataFormatterConfig();
        List<AirbyteStream> filteredAirbyteStreams = new ArrayList<>();

        if (dataFormatterConfig != null) {
            if (dataFormatterConfig.getConfigValue(BicycleBigQueryWrapper.MATCH_STREAMS_NAME) != null) {
                String commaSeparatedColumnNames = (String) dataFormatterConfig
                        .getConfigValue(BicycleBigQueryWrapper.MATCH_STREAMS_NAME);
                if (StringUtils.isNotEmpty(commaSeparatedColumnNames)) {
                    String[] matchStreamNames = commaSeparatedColumnNames.split("\\s*,\\s*");
                    for (AirbyteStream airbyteStream : airbyteCatalog.getStreams()) {
                        if (filterStream(matchStreamNames, airbyteStream.getName())) {
                            filteredAirbyteStreams.add(airbyteStream);
                        }
                    }
                    airbyteCatalog.getStreams().clear();
                    airbyteCatalog.getStreams().addAll(filteredAirbyteStreams);
                }
            }
        }

        return airbyteCatalog;
    }

    private boolean filterStream(String[] matchStreamNames, String currentStreamName) {
        if (matchStreamNames == null) {
            return true;
        }

        for (String matchStreamPattern : matchStreamNames) {
            if (currentStreamName.matches(matchStreamPattern)) {
                return true;
            }
        }

        return false;
    }

    protected AtomicBoolean getStopConnectorBoolean() {
        return stopConnectorBoolean;
    }

 /*   private DataFormatter getDataFormatter(JsonNode config) {

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
    }*/

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
