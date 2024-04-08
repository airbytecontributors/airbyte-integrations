package io.airbyte.integrations.source.event.snowflake;

import static io.airbyte.integrations.bicycle.base.integration.MetricAsEventsGenerator.SOURCE_TYPE;
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
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.integrations.source.event.bigquery.BigQueryEventSourceConfig;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.SyncMode;
import io.bicycle.entity.mapping.SourceFieldMapping;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.integration.connector.runtime.BackFillConfiguration;
import io.bicycle.integration.connector.runtime.RuntimeConfig;
import io.bicycle.server.event.mapping.UserServiceFieldDef;
import io.bicycle.server.event.mapping.UserServiceFieldsList;
import io.bicycle.server.event.mapping.UserServiceFieldsRule;
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
public class SnowflakeEventSource extends BaseEventConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeEventSource.class);
    private static final String DATE_TIME_FORMAT_FALLBACK_PATTERN = "yyyy-MM-dd HH:mm:ss z";

    public static final TagEncodedMetricName SNOWFLAKE_CYCLE_TIME = TagEncodedMetricName
            .decode("connector_cycle");

    public static final TagEncodedMetricName SNOWFLAKE_PULL_RECORDS_TIME = TagEncodedMetricName
            .decode("connector_pull_records");

    public static final TagEncodedMetricName SNOWFLAKE_PROCESS_RECORDS_TIME = TagEncodedMetricName
            .decode("connector_process_records");

    public static final TagEncodedMetricName SNOWFLAKE_PUBLISH_RECORDS_TIME = TagEncodedMetricName
            .decode("connector_publish_records");
    public static final String STREAM_NAME_TAG = "streamName";
    private BicycleSnowflakeWrapper bicycleSnowflakeWrapper = new BicycleSnowflakeWrapper();
    private AtomicBoolean stopConnectorBoolean = new AtomicBoolean(false);
    private AtomicLong totalRecordsProcessed = new AtomicLong(0);
    private SnowflakeEventSourceMetricGenerator snowflakeEventSourceMetricGenerator;

    public SnowflakeEventSource(SystemAuthenticator systemAuthenticator,
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
        super.stopEventConnector("Snowflake Event Connector Stopped manually", JobExecutionStatus.success);
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
            LOGGER.info("Inside preview with config {} and catalog {} and state {}", config, catalog, state);

            SnowflakeEventSourceConfig snowflakeEventSourceConfig = new SnowflakeEventSourceConfig(config);
            LOGGER.info("Config returned is {}", snowflakeEventSourceConfig);

            BigQueryEventSourceConfig bigQueryEventSourceConfig = new BigQueryEventSourceConfig(config,
                    snowflakeEventSourceConfig.getCursorField());
            bicycleSnowflakeWrapper = new BicycleSnowflakeWrapper(bigQueryEventSourceConfig);

            AutoCloseableIterator<AirbyteMessage> messagesIterator =
                    bicycleSnowflakeWrapper.read(config, catalog, state);

            return AutoCloseableIterators.fromIterator(new AbstractIterator<>() {
                @Override
                protected AirbyteMessage computeNext() {
                    if (messagesIterator.hasNext()) {
                        AirbyteMessage message = messagesIterator.next();
                        if (message.getType().equals(AirbyteMessage.Type.RECORD) && message.getRecord() != null) {
                            JsonNode jsonNode = message.getRecord().getData();
                            message.getRecord().setData(jsonNode);
                        }
                        return message;
                    }
                    return endOfData();
                }
            });

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AutoCloseableIterator<AirbyteMessage> doRead(JsonNode config, ConfiguredAirbyteCatalog catalog,
                                                        JsonNode state) throws Exception {

        LOGGER.info("Inside read with config {}, catalog {}, state {}", config, catalog,
                state);

        String connectorId = additionalProperties.containsKey("bicycleConnectorId")
                ? additionalProperties.get("bicycleConnectorId").toString() : "";

        SnowflakeEventSourceConfig snowflakeEventSourceConfig = new SnowflakeEventSourceConfig(config);
        LOGGER.info("{} Config returned is {}", connectorId, snowflakeEventSourceConfig);

        AuthInfo authInfo = bicycleConfig.getAuthInfo();
        String eventSourceType = additionalProperties.containsKey("bicycleEventSourceType") ?
                additionalProperties.get("bicycleEventSourceType").toString() : CommonUtils.UNKNOWN_EVENT_CONNECTOR;

        eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);

        List<UserServiceMappingRule> userServiceMappingRules =
                this.getUserServiceMappingRules(authInfo, eventSourceInfo);

        SourceFieldMapping sourceFieldMapping = getSourceFieldMapping(userServiceMappingRules);

        String cursorField = getCursorField(snowflakeEventSourceConfig, sourceFieldMapping);
        if (cursorField == null) {
            throw new RuntimeException("Cursor field cannot be null, its neither defined in connector config," +
                    "not startTimeMicros is defined in the mapping rule");
        } else {
            LOGGER.info("Cursor field identified is {} and sourceFieldMapping is {}", cursorField,
                    sourceFieldMapping);
        }

        if (snowflakeEventSourceConfig.isIncremental()) {
            updateSyncMode(catalog, cursorField);
        }

        stopConnectorBoolean.set(false);

        LOGGER.info("Inside doRead for connector {} with config {} and catalog {}", connectorId,
                config, catalog);
        BigQueryEventSourceConfig bigQueryEventSourceConfig = new BigQueryEventSourceConfig(config,
                snowflakeEventSourceConfig.getCursorField());
        bicycleSnowflakeWrapper = new BicycleSnowflakeWrapper(bigQueryEventSourceConfig);
        ScheduledExecutorService ses = Executors.newScheduledThreadPool(3);


        try {
            try {

                if (snowflakeEventSourceMetricGenerator == null) {
                    snowflakeEventSourceMetricGenerator =
                            new SnowflakeEventSourceMetricGenerator(bicycleConfig,
                                    eventSourceInfo, config, bicycleEventPublisher, this,
                                    snowflakeEventSourceConfig);
                }

                ses.scheduleAtFixedRate(snowflakeEventSourceMetricGenerator, 60, 120, TimeUnit.SECONDS);

                LOGGER.info("Successfully started SnowflakeEventSourceMetricGenerator");
            } catch (Exception e) {
                LOGGER.error("Unable to start SnowflakeEventSourceMetricGenerator", e);
            }

            try {
                eventConnectorJobStatusNotifier.sendStatus(JobExecutionStatus.processing,
                        "Snowflake Event Connector started Successfully", connectorId, 0, authInfo);
            } catch (Exception e) {
                LOGGER.error("Exception while getting sending Snowflake event connector status");
            }

            LOGGER.info("Trying to get the state for Snowflake with connectorId {}", connectorId);
            String savedState = getStateAsString(authInfo, connectorId);
            LOGGER.info("Fetching state {} for Snowflake for connectorId {}", savedState, connectorId);

            if (!StringUtils.isEmpty(savedState)) {
                state = Jsons.deserialize(savedState);
            }
            LOGGER.info("Current state from Snowflake {}", state);
            JsonNode updatedState = null;

            runtimeConfig = connectorConfigManager != null ? connectorConfigManager
                    .getRuntimeConfig(authInfo, connectorId) : RuntimeConfig.getDefaultInstance();

            BackFillConfiguration backFillConfiguration = runtimeConfig.getBackFillConfig();
            boolean stopConnector = false;

            while (!this.getStopConnectorBoolean().get()) {
                authInfo = bicycleConfig.getAuthInfo();

                if (stopConnector) {
                    stopConnector(authInfo, connectorId, state);
                    break;
                }

                Timer.Context consumerCycleTimer = MetricUtils.getMetricRegistry().timer(
                        SNOWFLAKE_CYCLE_TIME
                                .withTags(SOURCE_ID, bicycleConfig.getConnectorId())
                                .toString()
                ).time();

                List<JsonNode> jsonEvents = new ArrayList<>();
                userServiceMappingRules =
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


                AutoCloseableIterator<AirbyteMessage> iterator = bicycleSnowflakeWrapper.read(config, catalog, state);

                getRecordsTimer.stop();

                while (iterator.hasNext()) {

                    AirbyteMessage message = iterator.next();
                    final boolean isState = message.getType() == AirbyteMessage.Type.STATE;
                    if (isState) {
                        AirbyteStateMessage currentState = message.getState();
                        String currentStateAsString = objectMapper.writeValueAsString(currentState.getData());
                        LOGGER.info("{} Found state message {}", connectorId, currentStateAsString);
                        updatedState = Jsons.deserialize(currentStateAsString);
                        continue;
                    }
                    if (message.getRecord() != null) {
                        JsonNode jsonNode = message.getRecord().getData();
                        Long backFillFieldValue = getBackFillFieldValue(authInfo, snowflakeEventSourceConfig, jsonNode,
                                sourceFieldMapping);
                        stopConnector = shouldStopConnector(stopConnector, backFillConfiguration, connectorId,
                                backFillFieldValue);
                        boolean shouldProcessRecord = shouldProcessRecord(stopConnector, backFillFieldValue,
                                backFillConfiguration);
                        if (shouldProcessRecord) {
                            jsonEvents.add(jsonNode);
                        }
                    } else {
                        LOGGER.warn("Message is not of type record but {}", message.getType());
                    }
                }

                LOGGER.info("Read {} messages for connector Id {}", jsonEvents.size(), connectorId);
                if (jsonEvents.size() == 0) {
                    state = updatedState;
                    continue;
                }

                EventProcessorResult eventProcessorResult = null;

                try {
                    Timer.Context processRecordsTimer = MetricUtils.getMetricRegistry().timer(
                            SNOWFLAKE_PROCESS_RECORDS_TIME
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
                            SNOWFLAKE_PUBLISH_RECORDS_TIME
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
            LOGGER.info("Closed server connection for snowflake");
        }

        return null;
    }

    private void stopConnector(AuthInfo authInfo, String connectorId, JsonNode state) {
        if (state != null) {
            ObjectNode updatedState = (ObjectNode) state;
            updatedState.put(TOTAL_RECORDS, totalRecordsProcessed.longValue());
            setStateAsString(authInfo, connectorId, updatedState);
        } else {

        }
        stopEventConnector();
    }

    public boolean shouldStopConnector(boolean stopConnector, BackFillConfiguration backFillConfiguration,
                                       String connectorId, Long backFillFieldValue) {
        if (stopConnector) {
            return true;
        }
        if (isBackFillDone(backFillConfiguration, backFillFieldValue)) {
            LOGGER.info("BackFill is done {}", connectorId);
            return true;
        }
        return false;
    }

    public boolean shouldProcessRecord(boolean stopConnector, Long backFillFieldValue,
                                       BackFillConfiguration backFillConfiguration) {

        if (stopConnector) {
            return false;
        }

        if (!BackFillConfiguration.getDefaultInstance().equals(backFillConfiguration)) {
            if (backFillFieldValue != null) {
                if (!shouldContinue(backFillConfiguration, backFillFieldValue)) {
                    return false;
                }
            }
        }
        return true;
    }

    private String getCursorField(SnowflakeEventSourceConfig snowflakeEventSourceConfig,
                                  SourceFieldMapping sourceFieldMapping) {

        if (StringUtils.isNotEmpty(snowflakeEventSourceConfig.getCursorField())) {
            return snowflakeEventSourceConfig.getCursorField();
        }

        if (sourceFieldMapping == null) {
            return null;
        }
        String jsonPath = sourceFieldMapping.getValueMapping().getJsonPath();
        return extractFieldNameFromJsonPath(jsonPath);
    }

    private String extractFieldNameFromJsonPath(String jsonPath) {
        return jsonPath.substring(2);
    }

    private Long getBackFillFieldValue(AuthInfo authInfo, SnowflakeEventSourceConfig snowflakeEventSourceConfig,
                                       JsonNode jsonNode, SourceFieldMapping sourceFieldMapping) {

        try {
            JsonRawEvent jsonRawEvent = new JsonRawEvent(jsonNode);
            Long valueInMicros =
                    (Long) jsonRawEvent.getFieldValue(sourceFieldMapping, Collections.emptyMap(), authInfo);
            Long valueInMillis = valueInMicros / 1000;
            return valueInMillis;
        } catch (Exception e) {
            LOGGER.error("Unable to get backfill field value", e);
        }

        return null;

  /*      String cursorField = snowflakeEventSourceConfig.getCursorField();
        String cursorFieldFormat = snowflakeEventSourceConfig.getCursorFieldFormat();
        try {
            if (StringUtils.isNotEmpty(cursorField)) {
                Object cursorFieldObj = jsonNode.get(cursorField);
                String cursorFieldValue = jsonNode.get(cursorField).asText();
                if (cursorFieldObj instanceof NumericNode) {
                    double doubleValue = Double.parseDouble(cursorFieldValue);
                    long result = (long) doubleValue;
                    cursorFieldValue = String.valueOf(result);
                }
                if (StringUtils.isNotEmpty(cursorFieldFormat)) {
                    return convertStringToTimestamp(cursorFieldValue, cursorFieldFormat);
                } else {
                    return Long.parseLong(cursorFieldValue);
                }
            } else {
                JsonRawEvent jsonRawEvent = new JsonRawEvent(jsonNode);
                Long value = (Long) jsonRawEvent.getFieldValue(sourceFieldMapping, Collections.emptyMap(), authInfo);
                return value;
            }
        } catch (Exception e) {
            LOGGER.error("Unable to get cursor field value", e);
        }*/
    }

    public ConfiguredAirbyteCatalog updateSyncMode(ConfiguredAirbyteCatalog catalog, String cursorFieldName) {
        for (ConfiguredAirbyteStream stream : catalog.getStreams()) {
            stream.setSyncMode(SyncMode.INCREMENTAL);
            if (stream.getCursorField().size() == 0) {
                stream.getCursorField().add(cursorFieldName);
            }
        }
        return catalog;
    }

    @Override
    public AirbyteConnectionStatus check(JsonNode config) throws Exception {
        LOGGER.info("Inside check with config {}", config);
        return bicycleSnowflakeWrapper.check(config);
    }

    @Override
    public AirbyteCatalog discover(JsonNode config) throws Exception {
        LOGGER.info("Inside discover with config {}", config);
        return bicycleSnowflakeWrapper.discover(config);
    }

    protected AtomicBoolean getStopConnectorBoolean() {
        return stopConnectorBoolean;
    }


    private SourceFieldMapping getSourceFieldMapping(List<UserServiceMappingRule> userServiceMappingRules) {
        UserServiceFieldDef startTimeFieldDef = null;
        boolean found = false;
        for (UserServiceMappingRule userServiceMappingRule : userServiceMappingRules) {
            UserServiceFieldsRule userServiceFields = userServiceMappingRule.getUserServiceFields();
            List<UserServiceFieldDef> commonFieldsList = userServiceFields.getCommonFieldsList();
            for (UserServiceFieldDef userServiceFieldDef : commonFieldsList) {
                if (userServiceFieldDef.getPredefinedFieldType().equals("startTimeMicros")) {
                    startTimeFieldDef = userServiceFieldDef;
                    found = true;
                    break;
                }
            }
            if (found) {
                break;
            }

            Map<String, UserServiceFieldsList> userServiceFieldsMap = userServiceFields.getUserServiceFieldsMap();
            for (String key : userServiceFieldsMap.keySet()) {
                UserServiceFieldsList userServiceFieldsList = userServiceFieldsMap.get(key);
                for (UserServiceFieldDef userServiceFieldDef : userServiceFieldsList.getUserServiceFieldList()) {
                    if (userServiceFieldDef.getPredefinedFieldType().equals("startTimeMicros")) {
                        startTimeFieldDef = userServiceFieldDef;
                        found = true;
                        break;
                    }
                }
                if (found) {
                    break;
                }
            }
            if (found) {
                break;
            }
        }
        if (startTimeFieldDef == null) {
            return null;
        }

        SourceFieldMapping sourceFieldMapping = startTimeFieldDef.getFieldMapping();
        return sourceFieldMapping;

    }


}
