package io.airbyte.integrations.source.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.BicycleAuthInfo;
import io.airbyte.integrations.bicycle.base.integration.BicycleConfig;
import io.airbyte.protocol.models.*;
import java.io.IOException;
import java.util.*;

import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.server.event.mapping.models.converter.BicycleEventsResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.airbyte.integrations.source.elasticsearch.ElasticsearchConstants.*;
import static io.airbyte.integrations.source.elasticsearch.ElasticsearchInclusions.KEEP_LIST;
import static io.bicycle.server.event.mapping.constants.OTELConstants.TENANT_ID;

public class ElasticsearchSource extends BaseEventConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchSource.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private static boolean setBicycleEventProcessorFlag=false;

    /*
        Mapping from elasticsearch to Airbyte types
        Elasticsearch data types: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
        Airbyte data types: https://docs.airbyte.com/understanding-airbyte/supported-data-types/

        In Elasticsearch, there is no dedicated array data type.
        Any field can contain zero or more values by default, however,
        all values in the array must be of the same data type
    */
    private final Map<String, Object> ElasticSearchToAirbyte = new HashMap<>() {{
        // BINARY
        put("binary", Arrays.asList("string", "array"));

        // BOOLEAN
        put("boolean", Arrays.asList("boolean", "array"));

        // KEYWORD FAMILY
        put("keyword", Arrays.asList("string", "array", "number", "integer"));
        put("constant_keyword",  Arrays.asList("string", "array", "number", "integer"));
        put("wildcard",  Arrays.asList("string", "array", "number", "integer"));

        // NUMBERS
        put("long", Arrays.asList("integer", "array"));
        put("unsigned_long", Arrays.asList("integer", "array"));
        put("integer", Arrays.asList("integer", "array"));
        put("short", Arrays.asList("integer", "array"));
        put("byte", Arrays.asList("integer", "array"));
        put("double", Arrays.asList("number", "array"));
        put("float", Arrays.asList("number", "array"));
        put("half_float", Arrays.asList("number", "array"));
        put("scaled_float", Arrays.asList("number", "array"));

        // ALIAS
        /* Writes to alias field not supported by ES. Can be safely ignored */

        // DATES
        put("date", Arrays.asList("string", "array"));
        put("date_nanos", Arrays.asList("number", "array"));

        // OBJECTS AND RELATIONAL TYPES
        put("object", Arrays.asList("object", "array"));
        put("flattened", Arrays.asList("object", "array"));
        put("nested", Arrays.asList("object", "string"));
        put("join", Arrays.asList("object", "string"));

        // STRUCTURED DATA TYPES
        put("integer_range", Arrays.asList("object", "array"));
        put("float_range", Arrays.asList("object", "array"));
        put("long_range", Arrays.asList("object", "array"));
        put("double_range", Arrays.asList("object", "array"));
        put("date_range", Arrays.asList("object", "array"));
        put("ip_range", Arrays.asList("object", "array"));
        put("ip", Arrays.asList("string", "array"));
        put("version", Arrays.asList("string", "array"));
        put("murmur3", Arrays.asList("object", "array"));

        // AGGREGATE METRIC FIELD TYPES
        put("aggregate_metric_double", Arrays.asList("object", "array"));
        put("histogram", Arrays.asList("object", "array"));

        // TEXT SEARCH TYPES
        put("text", Arrays.asList("string", "array"));
        put("alias", Arrays.asList("string", "array"));
        put("search_as_you_type", Arrays.asList("string", "array"));
        put("token_count", Arrays.asList("integer", "array"));

        // DOCUMENT RANKING
        put("dense_vector", "array");
//        put("rank_feature", "integer"); THEY ARE PUTTING OBJECTS HERE AS WELL????

        // SPATIAL DATA TYPES (HARD TO HANDLE AS QUERYING MECHANISM IS BASED ON SHAPE, which has multiple fields)
        put("geo_point", Arrays.asList("object", "array"));
        put("geo_shape", Arrays.asList("object", "array"));
        put("shape", Arrays.asList("object", "array"));
        put("point", Arrays.asList("object", "array"));
    }};

    public static void main(String[] args) throws Exception {
        final var Source = new ElasticsearchSource();
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

        JsonNode mappingsNode = mapper.convertValue(mappings, JsonNode.class);
        List<AirbyteStream> streams = new ArrayList<>();

        for(var index: indices) {
            JsonNode JSONSchema = mappingsNode.get(index).get("sourceAsMap");
            JsonNode formattedJSONSchema = formatJSONSchema(JSONSchema);
            AirbyteStream stream = new AirbyteStream();
            stream.setSupportedSyncModes(List.of(SyncMode.FULL_REFRESH));
            stream.setName(index);
            stream.setJsonSchema(formattedJSONSchema);
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

    /**
     * @param node JsonNode node which we want to format
     * @return JsonNode
     * @throws UnsupportedDatatypeException throws an exception if none of the types match
     */
    private JsonNode formatJSONSchema(JsonNode node) throws UnsupportedDatatypeException {
        if(node.isObject()) {
            if(!node.has("type") || node.has("properties")) {
                ((ObjectNode)node).put("type", "object");
            }
            else if(node.has("type") && node.get("type").getNodeType()==JsonNodeType.STRING) {
                retainAirbyteFieldsOnly(node);

                final String nodeType = node.get("type").textValue();

                if (ElasticSearchToAirbyte.containsKey(nodeType)) {
                    ((ObjectNode) node).remove("type");
                    ((ObjectNode) node).set("type", this.mapper.valueToTree(ElasticSearchToAirbyte.get(nodeType)));
                }
                else throw new UnsupportedDatatypeException("Cannot map unsupported data type to Airbyte data type: "+node.get("type").textValue());
            }
            node.fields().forEachRemaining(entry -> {
                try {
                    formatJSONSchema(entry.getValue());
                } catch (UnsupportedDatatypeException e) {
                    throw new RuntimeException(e);
                }
            });
            if(node.path("properties").path("type").getNodeType() == JsonNodeType.STRING) {
                ((ObjectNode)node.path("properties")).remove("type");
            }
            else if(node.has("properties")) {
                ((ObjectNode)node).set("type", this.mapper.valueToTree(Arrays.asList("array", "object")));
            }
        }
        else if(node.isArray()) {
            ArrayNode arrayNode = (ArrayNode) node;
            Iterator<JsonNode> temp = arrayNode.elements();
            while (temp.hasNext()) {
                formatJSONSchema(temp.next());
            }
        }
        return node;
    }

    private void retainAirbyteFieldsOnly(JsonNode jsonNode) {
        if(jsonNode instanceof ObjectNode) {
            ((ObjectNode)jsonNode).retain(KEEP_LIST);
        }
    }

    private AutoCloseableIterator<AirbyteMessage> readEntity(final JsonNode config, final ConfiguredAirbyteCatalog catalog, final JsonNode state, final ElasticsearchConnection connection) {
        final List<AutoCloseableIterator<AirbyteMessage>> iteratorList = new ArrayList<>();
        final JsonNode timeRange = config.has(TIME_RANGE)? config.get(TIME_RANGE): null;
        catalog.getStreams()
                .stream()
                .map(ConfiguredAirbyteStream::getStream)
                .forEach(stream -> {
                    AutoCloseableIterator<JsonNode> data = ElasticsearchUtils.getDataIterator(connection, stream, timeRange);
                    AutoCloseableIterator<AirbyteMessage> messageIterator = ElasticsearchUtils.getMessageIterator(data, stream.getName());
                    iteratorList.add(messageIterator);
                });
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
        String uniqueIdentifier = UUID.randomUUID().toString();
        String token = additionalProperties.containsKey("bicycleToken") ? additionalProperties.get("bicycleToken").toString() : "";
        String connectorId = additionalProperties.containsKey("bicycleConnectorId") ? additionalProperties.get("bicycleConnectorId").toString() : "";
        String eventSourceType= additionalProperties.containsKey("bicycleEventSourceType") ? additionalProperties.get("bicycleEventSourceType").toString() : "";

        BicycleConfig bicycleConfig = new BicycleConfig(serverURL, token, connectorId, uniqueIdentifier);
        if (!setBicycleEventProcessorFlag) {
            setBicycleEventProcessor(bicycleConfig);
            setBicycleEventProcessorFlag=true;
        }
        BicycleAuthInfo authInfo = new BicycleAuthInfo(bicycleConfig.getToken(), TENANT_ID);
        EventSourceInfo eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);
        ConfiguredAirbyteStream configuredAirbyteStream = catalog.getStreams().get(0);
        int sampledRecords = 0;
        final String index = configuredAirbyteStream.getStream().getName();

        LOGGER.info("======Starting read operation for elasticsearch index" + index + "=======");

        JsonNode timeRange = config.has(TIME_RANGE)? config.get(TIME_RANGE): null;
        if(timeRange!=null && !timeRange.has(TIME_FIELD)) {
            ((ObjectNode)timeRange).put(TIME_FIELD, "@timestamp");
        }
        try {
            // if timeRange not given
            String lastEnd;
            while(true) {
                lastEnd = new DateTime().toString();
                List<JsonNode> recordsList = connection.getRecords(index, timeRange);
                timeRange = updateTimeRange(timeRange, lastEnd);

                LOGGER.info("No of records read {}", recordsList.size());
                if (recordsList.size() == 0) continue;
                BicycleEventsResult eventProcessorResult = null;

                try {
                    List<RawEvent> rawEvents = this.convertRecordsToRawEvents(recordsList);
                    eventProcessorResult = convertRawEventsToBicycleEvents(authInfo,eventSourceInfo,rawEvents);
                    sampledRecords += recordsList.size();
                    LOGGER.info("Finished publishing {} events, total events {}", recordsList.size(), sampledRecords);
                } catch (Exception exception) {
                    LOGGER.error("Unable to convert raw records to bicycle events", exception);
                }

                try {
                    publishEvents(authInfo, eventSourceInfo, eventProcessorResult);
                } catch (Exception exception) {
                    LOGGER.error("Unable to publish bicycle events", exception);
                }
            }
        }
        catch(Exception exception) {
            LOGGER.error("Unable to publish bicycle events", exception);
        }
        finally {
            LOGGER.info("Closing server connection.");
            connection.close();
            LOGGER.info("Closed server connection.");
        }

    }

    protected List<RawEvent> convertRecordsToRawEvents(List<?> records) {
        Iterator<?> recordsIterator = (Iterator<?>) records.iterator();
        List<RawEvent> rawEvents = new ArrayList<>();
        while (recordsIterator.hasNext()) {
            JsonNode record = (JsonNode) recordsIterator.next();
            JsonRawEvent jsonRawEvent = new JsonRawEvent(record.asText());
            rawEvents.add(jsonRawEvent);
        }
        if (rawEvents.size() == 0) {
            return null;
        }
        return rawEvents;
    }

    private JsonNode updateTimeRange(JsonNode timeRange, final String lastEnd) {
        if(timeRange!=null) {
            ((ObjectNode)timeRange).put(FROM, lastEnd);
            ((ObjectNode)timeRange).put(TO, "now");
            return timeRange;
        }
        else {
            Map<String, String> tr = new HashMap<>() {{
                put(TIME_FIELD, "@timestamp");
                put(FROM, lastEnd);
                put(TO, "now");
            }};
            return mapper.convertValue(tr, JsonNode.class);
        }
    }
}