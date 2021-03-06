package io.airbyte.integrations.source.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.BicycleAuthInfo;
import io.airbyte.integrations.bicycle.base.integration.BicycleConfig;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.server.event.mapping.models.converter.BicycleEventsResult;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.airbyte.integrations.source.elasticsearch.ElasticsearchConstants.*;
import static io.airbyte.integrations.source.elasticsearch.typemapper.ElasticsearchTypeMapper.*;
import static io.bicycle.server.event.mapping.constants.OTELConstants.TENANT_ID;

public class ElasticsearchSource extends BaseEventConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchSource.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private boolean setBicycleEventProcessorFlag=false;

    public ElasticsearchSource(SystemAuthenticator systemAuthenticator, EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier) {
        super(systemAuthenticator, eventConnectorJobStatusNotifier);
    }

    public static void main(String[] args) throws Exception {
        final var Source = new ElasticsearchSource(null,null);
        LOGGER.info("starting Source: {}", ElasticsearchSource.class);
        new IntegrationRunner(Source).run(args);
        LOGGER.info("completed Source: {}", ElasticsearchSource.class);
    }

    @Override
    public AirbyteConnectionStatus check(JsonNode config) {
        final ConnectorConfiguration configObject = convertConfig(config);
        if (Objects.isNull(configObject.getEndpoint())) {
            return new AirbyteConnectionStatus()
                    .withStatus(AirbyteConnectionStatus.Status.FAILED).withMessage("endpoint must not be empty");
        }
        if (!configObject.getAuthenticationMethod().isValid()) {
            return new AirbyteConnectionStatus()
                    .withStatus(AirbyteConnectionStatus.Status.FAILED).withMessage("authentication options are invalid");
        }

        final ElasticsearchConnection connection = new ElasticsearchConnection(configObject);
        final var result = connection.checkConnection();
        try {
            connection.close();
        } catch (IOException e) {
            LOGGER.warn("failed while closing connection", e);
        }
        if (result) {
            return new AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.SUCCEEDED);
        } else {
            return new AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.FAILED).withMessage("failed to ping elasticsearch");
        }
    }

    @Override
    public AirbyteCatalog discover(JsonNode config) throws Exception {
        final ConnectorConfiguration configObject = convertConfig(config);
        final ElasticsearchConnection connection = new ElasticsearchConnection(configObject);
        final var indices = connection.userIndices();
        final var mappings = connection.getMappings(indices);

//        JsonNode mappingsNode = mapper.convertValue(mappings, JsonNode.class);
        List<AirbyteStream> streams = new ArrayList<>();

        for(var index: indices) {
            JsonNode JSONSchema = mapper.convertValue(mappings.get(index).sourceAsMap(), JsonNode.class);
            Set<String> timestampCandidates = getTimestampCandidateField(JSONSchema.get("properties"));
            LOGGER.info("Following fields can be used for timestamp queries: {}. @timestamp is recommended by default.", timestampCandidates);
            JsonNode formattedJSONSchema = formatJSONSchema(JSONSchema);
            AirbyteStream stream = new AirbyteStream();
            stream.setSupportedSyncModes(List.of(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL));
            stream.setName(index);
            stream.setJsonSchema(formattedJSONSchema);
            if(timestampCandidates.contains("@timestamp")) {
                stream.setSourceDefinedCursor(true);
                stream.setDefaultCursorField(List.of("@timestamp"));
            }
            else {
                stream.setSourceDefinedCursor(false);
            }
            streams.add(stream);
        }
        try {
            connection.close();
        } catch (IOException e) {
            LOGGER.warn("failed while closing connection", e);
        }
        return new AirbyteCatalog().withStreams(streams);
    }

    @Override
    public AutoCloseableIterator<AirbyteMessage> read(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) throws IOException {
        final ConnectorConfiguration configObject = convertConfig(config);
        final ElasticsearchConnection connection = new ElasticsearchConnection(configObject);

        if(config.has(CONNECTOR_TYPE) && config.get(CONNECTOR_TYPE).textValue().equals(ENTITY)) {
            return readEntity(config, catalog, state, connection);
        }
        else {
            // default: EVENT
            readEvent(config, catalog, state, connection);
        }

        return null;
    }

    private ConnectorConfiguration convertConfig(JsonNode config) {
        return mapper.convertValue(config, ConnectorConfiguration.class);
    }

    private AutoCloseableIterator<AirbyteMessage> readEntity(final JsonNode config, final ConfiguredAirbyteCatalog catalog, final JsonNode state, final ElasticsearchConnection connection) {
        final List<AutoCloseableIterator<AirbyteMessage>> iteratorList = new ArrayList<>();
        final JsonNode timeRange = config.has(TIME_RANGE)? config.get(TIME_RANGE): null;

        AirbyteStream stream = catalog.getStreams().get(0).getStream();
        LOGGER.debug("Stream {}, timeRange {}", stream, timeRange);
        AutoCloseableIterator<JsonNode> data = ElasticsearchUtils.getDataIterator(connection, stream, timeRange);
        AutoCloseableIterator<AirbyteMessage> messageIterator = ElasticsearchUtils.getMessageIterator(data, stream.getName());
        iteratorList.add(messageIterator);

        return AutoCloseableIterators
                .appendOnClose(AutoCloseableIterators.concatWithEagerClose(iteratorList), () -> {
                    LOGGER.info("Closing server connection.");
                    connection.close();
                    LOGGER.info("Closed server connection.");
                });
    }

    private void readEvent(final JsonNode config, final ConfiguredAirbyteCatalog catalog, final JsonNode state, final ElasticsearchConnection connection) throws IOException {
        Map<String, Object> additionalProperties = catalog.getAdditionalProperties();
        String serverURL = additionalProperties.containsKey("bicycleServerURL") ? additionalProperties.get("bicycleServerURL").toString() : "";
        String metricStoreURL = additionalProperties.containsKey("bicycleMetricStoreURL") ? additionalProperties.get("bicycleMetricStoreURL").toString() : "";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String token = additionalProperties.containsKey("bicycleToken") ? additionalProperties.get("bicycleToken").toString() : "";
        String connectorId = additionalProperties.containsKey("bicycleConnectorId") ? additionalProperties.get("bicycleConnectorId").toString() : "";
        String eventSourceType= additionalProperties.containsKey("bicycleEventSourceType") ? additionalProperties.get("bicycleEventSourceType").toString() : "";
        String tenantId = additionalProperties.containsKey("bicycleTenantId") ? additionalProperties.get("bicycleTenantId").toString() : "tenantId";;
        String isOnPrem = additionalProperties.get("isOnPrem").toString();
        boolean isOnPremDeployment = Boolean.parseBoolean(isOnPrem);

        BicycleConfig bicycleConfig = new BicycleConfig(serverURL, metricStoreURL, token, connectorId,uniqueIdentifier, tenantId,systemAuthenticator, isOnPremDeployment);
        setBicycleEventProcessor(bicycleConfig);
        EventSourceInfo eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);
        ConfiguredAirbyteStream configuredAirbyteStream = catalog.getStreams().get(0);
        int sampledRecords = 0;
        final String index = configuredAirbyteStream.getStream().getName();

        LOGGER.info("======Starting read operation for elasticsearch index" + index + "=======");

        JsonNode timeRange = config.has(TIME_RANGE)? config.get(TIME_RANGE): JsonNodeFactory.instance.objectNode();;
        if(timeRange!=null && !timeRange.has(TIME_FIELD)) {
            ((ObjectNode)timeRange).put(TIME_FIELD, "@timestamp");
        }
        AuthInfo authInfo = bicycleConfig.getAuthInfo();
        try {
            // if timeRange not given
            String lastEnd;
            eventConnectorJobStatusNotifier.sendStatus(JobExecutionStatus.processing,"Kafka Event Connector started Successfully", connectorId, authInfo);
            while(true) {
                final String latestDataTimestamp = connection.getLatestTimestamp(index, timeRange.path(TIME_FIELD).textValue());
                if(latestDataTimestamp.equals(timeRange.path(FROM).textValue())) {
                    LOGGER.info("No new data seen after timestamp: {}, querying again in 5 seconds", latestDataTimestamp);
                    TimeUnit.SECONDS.sleep(5);
                    continue;
                }
                ((ObjectNode)timeRange).put(TO, latestDataTimestamp);
                LOGGER.info("Getting data for time-field:{}, From:{}, To:{}", timeRange.path(TIME_FIELD).textValue(), timeRange.path(FROM).textValue(), timeRange.path(TO).textValue());
                List<JsonNode> recordsList = connection.getRecords(index, timeRange);
                timeRange = updateTimeRange(timeRange, latestDataTimestamp);
                LOGGER.info("No of records read {}", recordsList.size());
                if (recordsList.size() == 0) {
                    TimeUnit.SECONDS.sleep(5);
                    continue;
                }
                EventProcessorResult eventProcessorResult = null;
                try {
                    List<RawEvent> rawEvents = this.convertRecordsToRawEvents(recordsList);
                    eventProcessorResult = convertRawEventsToBicycleEvents(authInfo,eventSourceInfo,rawEvents);
                    sampledRecords += recordsList.size();
                } catch (Exception exception) {
                    LOGGER.error("Unable to convert raw records to bicycle events", exception);
                }

                try {
                    publishEvents(authInfo, eventSourceInfo, eventProcessorResult);
                    LOGGER.info("New events found:{}. Total events published:{}", recordsList.size(), sampledRecords);
                } catch (Exception exception) {
                    LOGGER.error("Unable to publish bicycle events", exception);
                }
            }
        }
        catch(Exception exception) {
            LOGGER.error("Exception while trying to fetch records from Elasticsearch", exception);
        }
        finally {
            eventConnectorJobStatusNotifier.removeConnectorIdFromMap(eventSourceInfo.getEventSourceId());
            eventConnectorJobStatusNotifier.sendStatus(JobExecutionStatus.failure,"Shutting down the ElasticSearch Event Connector", connectorId, authInfo);
            LOGGER.info("Closing server connection.");
            connection.close();
            LOGGER.info("Closed server connection.");
        }

    }

    public List<RawEvent> convertRecordsToRawEvents(List<?> records) {
        Iterator<?> recordsIterator = (Iterator<?>) records.iterator();
        List<RawEvent> rawEvents = new ArrayList<>();
        while (recordsIterator.hasNext()) {
            JsonNode record = (JsonNode) recordsIterator.next();
            JsonRawEvent jsonRawEvent = new JsonRawEvent(record.toString());
            rawEvents.add(jsonRawEvent);
        }
        if (rawEvents.size() == 0) {
            return null;
        }
        return rawEvents;
    }

    @Override
    public AutoCloseableIterator<AirbyteMessage> preview(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) {
        final ConnectorConfiguration configObject = convertConfig(config);
        final ElasticsearchConnection connection = new ElasticsearchConnection(configObject);
        final AirbyteConnectionStatus check = check(config);
        if (check.getStatus().equals(AirbyteConnectionStatus.Status.FAILED)) {
            throw new RuntimeException("Unable to establish a connection: " + check.getMessage());
        }
        return readEntity(config, catalog, state, connection);
    }

    private JsonNode updateTimeRange(JsonNode timeRange, final String lastEnd) {
        if(timeRange!=null) {
            ((ObjectNode)timeRange).put("method", "custom");
            ((ObjectNode)timeRange).put(FROM, lastEnd);
            return timeRange;
        }
        else {
            Map<String, String> tr = new HashMap<>() {{
                put("method", "custom");
                put(TIME_FIELD, "@timestamp");
                put(FROM, lastEnd);
            }};
            return mapper.convertValue(tr, JsonNode.class);
        }
    }
}