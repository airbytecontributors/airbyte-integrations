package io.airbyte.integrations.source.elasticsearch;

import static io.airbyte.integrations.bicycle.base.integration.CommonConstants.CONNECTOR_RECORDS_PULL_METRIC;
import static io.airbyte.integrations.bicycle.base.integration.MetricAsEventsGenerator.SOURCE_TYPE;
import static io.airbyte.integrations.source.elasticsearch.ElasticsearchConstants.CONNECTOR_TYPE;
import static io.airbyte.integrations.source.elasticsearch.ElasticsearchConstants.ENTITY;
import static io.airbyte.integrations.source.elasticsearch.ElasticsearchConstants.FROM;
import static io.airbyte.integrations.source.elasticsearch.ElasticsearchConstants.TIME_FIELD;
import static io.airbyte.integrations.source.elasticsearch.ElasticsearchConstants.TIME_RANGE;
import static io.bicycle.integration.common.constants.EventConstants.SOURCE_ID;
import static io.bicycle.integration.common.constants.EventConstants.THREAD_ID;
import ai.apptuit.ml.utils.MetricUtils;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.CommonUtils;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.server.event.mapping.UserServiceMappingRule;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchSource extends BaseEventConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchSource.class);
    public static final String STATE = "state";
    public static final String ELASTIC_LAG = "elastic_lag";
    private final ObjectMapper mapper = new ObjectMapper();
    private int totalRecordsConsumed = 0;
    private AtomicBoolean stopConnectorBoolean = new AtomicBoolean(false);

    public ElasticsearchSource(SystemAuthenticator systemAuthenticator,
                               EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier,
                               ConnectorConfigManager connectorConfigManager) {
        super(systemAuthenticator, eventConnectorJobStatusNotifier, connectorConfigManager);
    }

    @Override
    protected int getTotalRecordsConsumed() {
        return totalRecordsConsumed;
    }

    public static void main(String[] args) throws Exception {
        final var Source = new ElasticsearchSource(null,null, null);
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
        ElasticsearchConnector elasticsearchConnector = new ElasticsearchConnector();
        RestClientBuilder restClientBuilder = elasticsearchConnector.createDefaultBuilder(configObject);
        RestClient restClient = restClientBuilder.build();
        final var result = elasticsearchConnector.testConnection(restClient);

        try {
            restClient.close();
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
        List<AirbyteStream> streams = new ArrayList<>();

        AirbyteStream stream = CatalogHelpers
                .createAirbyteStream(configObject.getIndexPattern(), Field.of("value", JsonSchemaType.STRING));
        streams.add(stream);

        return new AirbyteCatalog().withStreams(streams);

      /*  final ElasticsearchConnection connection = new ElasticsearchConnection(configObject);
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
*/    }

    @Override
    public AutoCloseableIterator<AirbyteMessage> doRead(
            JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) throws Exception {
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

    private void readEvent(final JsonNode config, final ConfiguredAirbyteCatalog catalog, final JsonNode state, final ElasticsearchConnection connection) throws Exception {
        Map<String, Object> additionalProperties = catalog.getAdditionalProperties();
        stopConnectorBoolean.set(false);
        String serverURL = additionalProperties.containsKey("bicycleServerURL") ? additionalProperties.get("bicycleServerURL").toString() : "";
        String metricStoreURL = additionalProperties.containsKey("bicycleMetricStoreURL") ? additionalProperties.get("bicycleMetricStoreURL").toString() : "";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String token = additionalProperties.containsKey("bicycleToken") ? additionalProperties.get("bicycleToken").toString() : "";
        String connectorId = additionalProperties.containsKey("bicycleConnectorId") ? additionalProperties.get("bicycleConnectorId").toString() : "";
        String eventSourceType= additionalProperties.containsKey("bicycleEventSourceType") ? additionalProperties.get("bicycleEventSourceType").toString() : CommonUtils.UNKNOWN_EVENT_CONNECTOR;
        String tenantId = additionalProperties.containsKey("bicycleTenantId") ? additionalProperties.get("bicycleTenantId").toString() : "tenantId";;
        String isOnPrem = additionalProperties.get("isOnPrem").toString();
        boolean isOnPremDeployment = Boolean.parseBoolean(isOnPrem);

        BicycleConfig bicycleConfig = new BicycleConfig(serverURL, metricStoreURL, token, connectorId,uniqueIdentifier, tenantId, systemAuthenticator, isOnPremDeployment);
        setBicycleEventProcessorAndPublisher(bicycleConfig);

        eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);
        ConfiguredAirbyteStream configuredAirbyteStream = catalog.getStreams().get(0);
        final String index = configuredAirbyteStream.getStream().getName();

        LOGGER.info("======Starting read operation for elasticsearch index" + index + "=======");

        final ConnectorConfiguration configObject = convertConfig(config);
        ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);


        AuthInfo authInfo = bicycleConfig.getAuthInfo();

        try {

            ElasticMetricsGenerator elasticMetricsGenerator = new ElasticMetricsGenerator(bicycleConfig,
                    eventSourceInfo, config, bicycleEventPublisher, this);
            InMemoryConsumer inMemoryConsumer = new InMemoryConsumer(this,
                    bicycleConfig, eventSourceInfo, elasticMetricsGenerator, 1);
            ElasticsearchConnector elasticsearchConnector = new ElasticsearchConnector(inMemoryConsumer, bicycleConfig, getConnectorConfigManager());

            ses.scheduleAtFixedRate(elasticMetricsGenerator, 60, 30, TimeUnit.SECONDS);
            try {
                eventConnectorJobStatusNotifier.sendStatus(JobExecutionStatus.processing,
                    "Elastic Event Connector started Successfully", connectorId, 0, authInfo);
            } catch (Exception e) {
                LOGGER.error("Exception while getting sending elastic event connector status");
            }

            RestClientBuilder restClientBuilder = elasticsearchConnector.createDefaultBuilder(configObject);
            RestClient restClient = restClientBuilder.build();
            long dataLateness = configObject.getDataLateness();
            long pollFrequency = configObject.getPollFrequency();
            String queryLine = configObject.getQueryWithIndexPattern();

            LOGGER.info("Trying to get the state for elastic search");
            AirbyteStateMessage airbyteStateMessage = getState(authInfo, connectorId);
            LOGGER.info("Fetching state from elastic search {}", airbyteStateMessage);
            long currentState = System.currentTimeMillis();
            if (airbyteStateMessage  != null) {
                JsonNode jsonNode = airbyteStateMessage.getData();
                if (jsonNode != null && jsonNode.has(STATE)) {
                    currentState = jsonNode.get(STATE).longValue();
                    //safer side check
                    if (currentState == 0) {
                        currentState = System.currentTimeMillis();
                    }
                }
            }
            LOGGER.info("Current state from elastic search {}", currentState);
            long startEpoch = currentState - dataLateness - pollFrequency;
            startEpoch -= startEpoch % pollFrequency;
            long endEpoch = startEpoch + pollFrequency;

            while (!this.getStopConnectorBoolean().get()) {
                inMemoryConsumer.rescheduleIfStopped();
                List<UserServiceMappingRule> rules =
                        getUserServiceMappingRules(authInfo, eventSourceInfo);
               //This means we are unable to download the configs because of some exception in config store.
                if (rules == null) {
                    continue;
                }
                Timer.Context timer = MetricUtils.getMetricRegistry().timer(
                        CONNECTOR_RECORDS_PULL_METRIC
                                .withTags(SOURCE_ID, bicycleConfig.getConnectorId())
                                .withTags(SOURCE_TYPE, eventSourceInfo.getEventSourceType())
                                .toString()
                ).time();
                elasticsearchConnector.search(restClient, startEpoch, endEpoch, queryLine, configObject.getPageSize(), false);
                timer.stop();
                startEpoch = endEpoch;
                endEpoch = startEpoch + pollFrequency;
                while ((System.currentTimeMillis() - dataLateness) < endEpoch) {
                    //Added a while loop because sometimes the thread seems to be waking 2-3 seconds before time
                    long sleepTime = endEpoch - (System.currentTimeMillis() - dataLateness);
                    LOGGER.info("Sleeping: {}", sleepTime);
                    if (sleepTime > 0) {
                        Thread.sleep(sleepTime);
                    }
                }
                //Otherwise authinfo might get expired
                authInfo = bicycleConfig.getAuthInfo();
            }
            LOGGER.info("Shutting down the Elasticsearch Event Connector manually for connector {}", bicycleConfig.getConnectorId());
        } finally {
            ses.shutdown();
            LOGGER.info("Closing server connection.");
            connection.close();
            LOGGER.info("Closed server connection.");
        }

    }

    public List<RawEvent> convertRecordsToRawEventsInternal(List<?> records) {
        Iterator<?> recordsIterator = (Iterator<?>) records.iterator();
        List<RawEvent> rawEvents = new ArrayList<>();
        while (recordsIterator.hasNext()) {
            JsonNode record = (JsonNode) recordsIterator.next();
            JsonRawEvent jsonRawEvent = createJsonRawEvent(record);
            rawEvents.add(jsonRawEvent);
        }
        return rawEvents;
    }

    @Override
    public AutoCloseableIterator<AirbyteMessage> preview(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) {
        final ConnectorConfiguration configObject = convertConfig(config);
        final ElasticsearchConnector elasticsearchConnector = new ElasticsearchConnector();
        final AirbyteConnectionStatus check = check(config);
        if (check.getStatus().equals(AirbyteConnectionStatus.Status.FAILED)) {
            throw new RuntimeException("Unable to establish a connection: " + check.getMessage());
        }
        List<JsonNode> jsonNodes = elasticsearchConnector.getPreviewRecords(configObject);

        AutoCloseableIterator<JsonNode> data = AutoCloseableIterators.fromIterator(jsonNodes.iterator());
        return  ElasticsearchUtils.getMessageIterator(data, configObject.getIndexPattern());
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

}