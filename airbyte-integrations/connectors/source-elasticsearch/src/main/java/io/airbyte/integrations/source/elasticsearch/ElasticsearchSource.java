package io.airbyte.integrations.source.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.inception.common.client.impl.GenericApiClient;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.bicycle.base.integration.*;
import io.airbyte.protocol.models.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import io.bicycle.entity.mapping.api.ConnectionServiceClient;

import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.integration.connector.SyncDataRequest;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.airbyte.integrations.source.elasticsearch.ElasticsearchConstants.*;
import static io.airbyte.integrations.source.elasticsearch.typemapper.ElasticsearchTypeMapper.*;

public class ElasticsearchSource extends BaseEventConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchSource.class);
    private final ObjectMapper mapper = new ObjectMapper();
    int totalRecordsConsumed = 0;
    private AtomicBoolean stopConnectorBoolean = new AtomicBoolean(false);
    private ConnectionServiceClient connectionServiceClient;
    private JsonNode localState=null;

    public ElasticsearchSource(SystemAuthenticator systemAuthenticator, EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier) {
        super(systemAuthenticator, eventConnectorJobStatusNotifier);
    }

    @Override
    protected int getTotalRecordsConsumed() {
        return totalRecordsConsumed;
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

    protected AtomicBoolean getStopConnectorBoolean() {
        return stopConnectorBoolean;
    }

    public void stopEventConnector() {
        stopConnectorBoolean.set(true);
        super.stopEventConnector("Shutting down the Elasticsearch Event Connector manually", JobExecutionStatus.success);
    }

    private AutoCloseableIterator<AirbyteMessage> readEntity(final JsonNode config, final ConfiguredAirbyteCatalog catalog, final JsonNode state, final ElasticsearchConnection connection) {
        final List<AutoCloseableIterator<AirbyteMessage>> iteratorList = new ArrayList<>();
        final JsonNode timeRange = config.has(TIME_RANGE)? config.get(TIME_RANGE): null;
        AirbyteStream stream = catalog.getStreams().get(0).getStream();
        LOGGER.debug("Stream {}, timeRange {}", stream, timeRange);
        AutoCloseableIterator<JsonNode> data = ElasticsearchUtils.getDataIteratorFullRefresh(connection, stream, timeRange);
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
        stopConnectorBoolean.set(false);
        Map<String, Object> additionalProperties = catalog.getAdditionalProperties();
        String eventSourceType= additionalProperties.containsKey("bicycleEventSourceType") ? additionalProperties.get("bicycleEventSourceType").toString() : "source-elasticsearch";
        BicycleConfig bicycleConfig = getBicycleConfig(additionalProperties, systemAuthenticator);
        setBicycleEventProcessorAndPublisher(bicycleConfig);
        eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);
        ConfiguredAirbyteStream configuredAirbyteStream = catalog.getStreams().get(0);
        int sampledRecords = 0;
        final String index = configuredAirbyteStream.getStream().getName();
        final String cursorField;

        if(config.get(SYNCMODE).get("method").textValue().equals("custom")) {
            cursorField = config.get(SYNCMODE).get(CURSORFIELD).textValue();
        }
        else {
            LOGGER.error("Currently elasticsearch connector only supports event sources");
            return;
        }

        this.connectionServiceClient = new ConnectionServiceClient(new GenericApiClient(), "https://api.dev.bicycle.io/");

        try {
            localState = mapper.readTree(connectionServiceClient.getReadStateConfigById(bicycleConfig.getAuthInfo(), bicycleConfig.getConnectorId()));
        }
        catch (Exception e) {
            LOGGER.error("Unable to set state from entity browser", e);
        }
        finally {
            LOGGER.info("Local state before read: {}", localState);
        }

        LOGGER.info("======Starting read operation for elasticsearch index" + index + "=======");

        try {
            eventConnectorJobStatusNotifier.sendStatus(JobExecutionStatus.processing,"Kafka Event Connector started Successfully", bicycleConfig.getConnectorId(), 0, authInfo);
            while(!this.getStopConnectorBoolean().get()) {
                String cursor = getStreamCursor(index); // always gets the local state only
                LOGGER.info("Getting data for stream: {}, cursor_field:{}, cursor:{}", index, cursorField, cursor);
                List<JsonNode> recordsList = connection.getRecordsUsingCursor(index, cursorField, cursor);
                LOGGER.info("No of records read {}", recordsList.size());
                if (recordsList.size() == 0 || (cursor!=null && recordsList.size()==1)) {
                    // recordsList size 1 with a valid cursor means the record having that cursor value is being returned
                    // wait for 5 seconds before querying
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
                    String newCursor = recordsList.get(recordsList.size()-1).get(cursorField).asText();
                    saveState(bicycleConfig, index, cursorField, newCursor);
                } catch (Exception exception) {
                    LOGGER.error("Unable to publish bicycle events", exception);
                }
                totalRecordsConsumed += recordsList.size();
            }
            LOGGER.info("Shutting down the Elasticsearch Event Connector manually for connector {}", bicycleConfig.getConnectorId());
        }
        catch(Exception exception) {
            LOGGER.error("Exception while trying to fetch records from Elasticsearch", exception);
            this.stopEventConnector("Shutting down the ElasticSearch Event Connector due to Exception",JobExecutionStatus.failure);
        }
        finally {
            LOGGER.info("Closing server connection.");
            connection.close();
            LOGGER.info("Closed server connection.");
        }
    }

    public List<RawEvent> convertRecordsToRawEvents(List<?> records) {
        Iterator<?> recordsIterator = records.iterator();
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

    @Override
    public AutoCloseableIterator<AirbyteMessage> syncData(JsonNode sourceConfig,
                                                          ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                                                          JsonNode readState,
                                                          SyncDataRequest syncDataRequest) {
        return null;
    }

    private BicycleConfig getBicycleConfig(Map<String, Object> additionalProperties,
                                           SystemAuthenticator systemAuthenticator) {
        String serverURL = additionalProperties.containsKey("bicycleServerURL") ? additionalProperties.get("bicycleServerURL").toString() : "";
        String metricStoreURL = additionalProperties.containsKey("bicycleMetricStoreURL") ? additionalProperties.get("bicycleMetricStoreURL").toString() : "";
        String token = additionalProperties.containsKey("bicycleToken") ? additionalProperties.get("bicycleToken").toString() : "";
        String connectorId = additionalProperties.containsKey("bicycleConnectorId") ? additionalProperties.get("bicycleConnectorId").toString() : "";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String tenantId = additionalProperties.containsKey("bicycleTenantId") ? additionalProperties.get("bicycleTenantId").toString() : "";
        String isOnPrem = additionalProperties.containsKey("isOnPrem") ? additionalProperties.get("isOnPrem").toString(): "true";
        boolean isOnPremDeployment = Boolean.parseBoolean(isOnPrem);
        return new BicycleConfig(serverURL, metricStoreURL, token, connectorId, uniqueIdentifier, tenantId,
                systemAuthenticator, isOnPremDeployment);
    }


    private String getStreamCursor(final String stream) {
        if(stream==null || localState==null || localState.isNull()) return null;
        if(!localState.path(stream).path("cursor").isMissingNode()) {
            return localState.get(stream).get("cursor").asText();
        }
        return null;
    }

    private void saveState(final BicycleConfig bicycleConfig, final String stream, final String cursorField, final String cursor) {
        JsonNode newState = (mapper.createObjectNode()).set(stream,  mapper.createObjectNode().put("cursor", cursor).put("cursorField", cursorField));
        try {
            connectionServiceClient.upsertReadStateConfig(bicycleConfig.getAuthInfo(), toUUID(bicycleConfig.getConnectorId()), newState.toString());
            this.localState = newState;
        }
        catch(Exception e) {
            throw new RuntimeException("Unable to save state on bicycle: ", e);
        }
    }

    // https://gitlab.apptuit.ai/inception/inception/server/integrations/connector-executor/-/blob/master/connector-executor-app/src/main/java/com/inception/connector/executor/utils/CommonUtils.java
    public static String toUUID(String configId) {
        if (StringUtils.isBlank(configId)) {
            return null;
        } else {
            String[] split = StringUtils.split(configId, ":");
            if (split.length == 1) {
                return split[0];
            } else if (split.length == 2) {
                return split[1];
            }
            return null;
        }
    }


}