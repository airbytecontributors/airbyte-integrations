package io.airbyte.integrations.source.event.bigquery.data.formatter;

import static io.airbyte.integrations.bicycle.base.integration.CommonConstants.CONNECTOR_LAG;
import static io.airbyte.integrations.bicycle.base.integration.MetricAsEventsGenerator.SOURCE_TYPE;
import static io.airbyte.integrations.source.event.bigquery.BigQueryEventSource.STREAM_NAME_TAG;
import static io.airbyte.integrations.source.event.bigquery.BigQueryStreamGetter.LAST_7_DAYS_MILLISECONDS;
import static io.bicycle.integration.common.constants.EventConstants.SOURCE_ID;
import ai.apptuit.metrics.client.TagEncodedMetricName;
import ai.apptuit.ml.utils.MetricUtils;
import com.codahale.metrics.CachedGauge;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import io.airbyte.integrations.source.event.bigquery.BicycleBigQueryWrapper;
import io.airbyte.integrations.source.event.bigquery.BigQueryEventSourceConfig;
import io.airbyte.integrations.source.relationaldb.models.DbState;
import io.airbyte.integrations.source.relationaldb.models.DbStreamState;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.SyncMode;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sumitmaheshwari
 * Created on 11/10/2023
 */
public class GoogleAnalyticsV4DataFormatter implements DataFormatter {
    private static final Logger logger = LoggerFactory.getLogger(GoogleAnalyticsV4DataFormatter.class.getName());
    private static final String EVENT_PARAMS_ATTRIBUTE = "event_params";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final List<String> processedStreams = new ArrayList<>();
    private Map<TagEncodedMetricName, Long> metricsMap = new HashMap<>();
    private DataFormatterConfig dataFormatterConfig;

    public GoogleAnalyticsV4DataFormatter(DataFormatterConfig dataFormatterConfig) {
        this.dataFormatterConfig = dataFormatterConfig;
    }

    @Override
    public JsonNode formatEvent(JsonNode jsonNode) {

        try {
            String[] columnNames = null;
            if (dataFormatterConfig.getConfigValue(BicycleBigQueryWrapper.UNMAP_COLUMNS_NAME) != null) {
                String commaSeparatedColumnNames = (String) dataFormatterConfig
                        .getConfigValue(BicycleBigQueryWrapper.UNMAP_COLUMNS_NAME);
                columnNames = commaSeparatedColumnNames.split("\\s*,\\s*");
            } else {
                columnNames = new String[] {EVENT_PARAMS_ATTRIBUTE};
            }

            for (String columnName : columnNames) {

                ArrayNode eventParams = (ArrayNode) jsonNode.get(columnName);
                JsonNode outputNode = objectMapper.createObjectNode();
                if (eventParams == null) {
                    continue;
                }
                for (int i = 0; i < eventParams.size(); i++) {
                    JsonNode node = eventParams.get(i);
                    String key = node.get("key").asText();
                    JsonNode valueNode = node.get("value");

                    // get non-null value
                    String stringValue =
                            valueNode.has("string_value") ? valueNode.get("string_value").asText(null) : null;
                    Integer intValue =
                            valueNode.has("int_value") ? valueNode.get("int_value").asInt(Integer.MIN_VALUE) : null;
                    Float floatValue = valueNode.has("float_value") ? valueNode.get("float_value").floatValue() : null;
                    Double doubleValue =
                            valueNode.has("double_value") ? valueNode.get("double_value").doubleValue() : null;
                    Long longValue = valueNode.has("long_value") ? valueNode.get("long_value").longValue() : null;

                    if (stringValue != null) {
                        ((ObjectNode) outputNode).put(key, stringValue);
                    } else if (intValue != null) {
                        ((ObjectNode) outputNode).put(key, intValue);
                    } else if (floatValue != null) {
                        ((ObjectNode) outputNode).put(key, floatValue);
                    } else if (doubleValue != null) {
                        ((ObjectNode) outputNode).put(key, doubleValue);
                    } else if (longValue != null) {
                        ((ObjectNode) outputNode).put(key, longValue);
                    }
                }
                ((ObjectNode) jsonNode).put(columnName, outputNode);
            }

            return jsonNode;
        } catch (Exception e) {
            logger.error("Unable to format event_params for google analytics v4", e);
        }

        return jsonNode;
    }

    @Override
    public String getCursorFieldName() {
        if (dataFormatterConfig.getConfigValue(BicycleBigQueryWrapper.CURSOR_FIELD) != null) {
            return (String) dataFormatterConfig.getConfigValue(BicycleBigQueryWrapper.CURSOR_FIELD);
        }
        return "event_timestamp";
    }

    @Override
    public String getCursorFieldValue(List<JsonNode> rawData) {
        long maxTimestamp = 0;

        try {
            for (JsonNode jsonNode : rawData) {
                long timestamp = jsonNode.get(getCursorFieldName()).asLong();
                if (timestamp > maxTimestamp) {
                    maxTimestamp = timestamp;
                }
            }
        } catch (Exception e) {
            logger.error("Unable to get cursor field value returning 0", e);
        }

        return String.valueOf(maxTimestamp);
    }

    @Override
    public ConfiguredAirbyteCatalog updateSyncMode(ConfiguredAirbyteCatalog catalog) {
        // ConfiguredAirbyteStream stream = catalog.getStreams().get(0);
        List<ConfiguredAirbyteStream> streams = catalog.getStreams();

        for (ConfiguredAirbyteStream stream : catalog.getStreams()) {
            String streamName = stream.getStream().getName();
 	        stream.setSyncMode(SyncMode.INCREMENTAL);
            if (stream.getCursorField().size() == 0) {
                stream.getCursorField().add(getCursorFieldName());
            }
           /* if (stream.getCursorField().size() == 0 && streamName.contains("events")) {
                stream.setSyncMode(SyncMode.INCREMENTAL);
                stream.getCursorField().add(getCursorFieldName());
            } else {
              stream.setSyncMode(SyncMode.FULL_REFRESH);
            }*/
        }
        return catalog;
    }

    @Override
    public ConfiguredAirbyteCatalog updateConfiguredAirbyteCatalogWithInterestedStreams(
            String connectorId, ConfiguredAirbyteCatalog catalog, List<AirbyteStream> availableStreams,
            BigQueryEventSourceConfig bigQueryEventSourceConfig) {

        try {
            int previousStreamCount = catalog.getStreams().size();
            List<String> previousStreamNames = new ArrayList<>();
            List<ConfiguredAirbyteStream> previousStreams = catalog.getStreams();
            for (ConfiguredAirbyteStream configuredAirbyteStream : previousStreams) {
                previousStreamNames.add(configuredAirbyteStream.getStream().getName());
            }

            List<ConfiguredAirbyteStream> interestedStreams = new ArrayList<>();
            List<String> newStreams = new ArrayList<>();

            interestedStreams = filterStreams(bigQueryEventSourceConfig, availableStreams);

            for (ConfiguredAirbyteStream airbyteStream : interestedStreams) {
                String name = airbyteStream.getStream().getName();
                newStreams.add(name);
            }

            catalog.getStreams().clear();
            catalog.getStreams().addAll(interestedStreams);
            int newStreamsCount = newStreams.size();
            if (newStreamsCount != previousStreamCount) {
                logger.info("Added or removed streams from catalog, previous stream {}, new streams {}",
                        previousStreamNames, newStreams);
            }
        } catch (Exception e) {
            logger.error("Unable to update catalog with interested streams for connector Id {} {} ", connectorId,
                    e);
        }

        catalog = updateSyncMode(catalog);

        return catalog;
    }

    public List<ConfiguredAirbyteStream> filterStreams(BigQueryEventSourceConfig bigQueryEventSourceConfig,
                                                       List<AirbyteStream> streams) {

        List<ConfiguredAirbyteStream> updateStreamsList = new ArrayList<>();
        List<String> streamNamesToPrint = new ArrayList<>();
        try {
            String projectId = bigQueryEventSourceConfig.getProjectId();
            String datasetName = bigQueryEventSourceConfig.getDatasetId();

            //BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
            ServiceAccountCredentials credentials = ServiceAccountCredentials
                    .fromStream(new ByteArrayInputStream
                            (bigQueryEventSourceConfig.getCredentialsJson().getBytes(StandardCharsets.UTF_8)));

            BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId)
                    .setCredentials(credentials)
                    .build().getService();

            long thresholdTimestamp = System.currentTimeMillis() - LAST_7_DAYS_MILLISECONDS;
            thresholdTimestamp = thresholdTimestamp * 1000;

            String[] matchStreamNames = null;
            if (dataFormatterConfig.getConfigValue(BicycleBigQueryWrapper.MATCH_STREAMS_NAME) != null) {
                String commaSeparatedColumnNames = (String) dataFormatterConfig
                        .getConfigValue(BicycleBigQueryWrapper.MATCH_STREAMS_NAME);
                if (StringUtils.isNotEmpty(commaSeparatedColumnNames)) {
                    matchStreamNames = commaSeparatedColumnNames.split("\\s*,\\s*");
                }
            }

            for (AirbyteStream stream : streams) {
                String tableName = stream.getName();
                try {

                  /*  if (!tableName.contains("intraday")) {
                        continue;
                    }*/
                    if (!doReadStream(matchStreamNames, tableName)) {
                        continue;
                    }

                    if (processedStreams.contains(tableName)) {
                        continue;
                    }
                    String query = "SELECT MAX(event_timestamp) AS maxTimestamp FROM `"
                            + projectId + "." + datasetName + "." + tableName + "`";

                    // Create a query configuration
                    QueryJobConfiguration.Builder queryConfigBuilder = QueryJobConfiguration.newBuilder(query)
                            .setPriority(QueryJobConfiguration.Priority.BATCH)
                            .setDefaultDataset(datasetName);

                    // Run the query
                    TableResult result = bigquery.query(queryConfigBuilder.build());

                    // Extract the record count
                    long maxTimeStamp = 0;
                    for (FieldValueList row : result.iterateAll()) {
                        maxTimeStamp = row.get("maxTimestamp").getLongValue();
                    }

                    if (maxTimeStamp != 0 && maxTimeStamp > thresholdTimestamp) {
                        updateStreamsList.add(createConfiguredAirbyteStream(stream));
                        streamNamesToPrint.add(stream.getName());
                    } else {
                      processedStreams.add(stream.getName());
                    }

                } catch (Exception e) {
                    logger.error("Unable to filter one of the stream {} because of {}", stream, e);
                    updateStreamsList.add(createConfiguredAirbyteStream(stream));
                }
            }

            logger.info("Filtered Streams {}", streamNamesToPrint);
            return updateStreamsList;

        } catch (Exception e) {
            logger.error("Unable to filter streams", e);
            for (AirbyteStream airbyteStream : streams) {
                updateStreamsList.add(createConfiguredAirbyteStream(airbyteStream));
                streamNamesToPrint.add(airbyteStream.getName());
            }
        }

        return updateStreamsList;
    }

    private boolean doReadStream(String[] matchStreamNames, String currentStreamName) {
        if (matchStreamNames == null) {
            return true;
        }

        for (String matchStreamPattern: matchStreamNames) {
            if (currentStreamName.matches(matchStreamPattern)) {
                return true;
            }
        }

        return false;
    }

    private ConfiguredAirbyteStream createConfiguredAirbyteStream(AirbyteStream stream) {
        ConfiguredAirbyteStream configuredAirbyteStream = new ConfiguredAirbyteStream();
        configuredAirbyteStream.setStream(stream);
        return configuredAirbyteStream;
    }

    @Override
    public void publishLagMetrics(EventSourceInfo eventSourceInfo, String stateAsString) {

        try {
            DbState dbState = objectMapper.readValue(stateAsString, DbState.class);
            List<DbStreamState> streams = dbState.getStreams();
            for (DbStreamState dbStreamState : streams) {
                List<String> cursorFields = dbStreamState.getCursorField();
                if (cursorFields == null || cursorFields.size() == 0) {
                    continue;
                }

                String cursorFieldValue = dbStreamState.getCursor();
                long lagInTime = (System.currentTimeMillis() * 1000 - Long.parseLong(cursorFieldValue)) / 1000;

                TagEncodedMetricName metricName = CONNECTOR_LAG
                        .withTags(SOURCE_ID, eventSourceInfo.getEventSourceId())
                        .withTags(STREAM_NAME_TAG, dbStreamState.getStreamName())
                        .withTags(SOURCE_TYPE, eventSourceInfo.getEventSourceType());

                metricsMap.put(metricName, lagInTime);
            }

            for (Map.Entry<TagEncodedMetricName, Long> metricsEntry : metricsMap.entrySet()) {

                MetricUtils.getMetricRegistry().gauge(metricsEntry.getKey().toString(),
                        () -> new CachedGauge(15, TimeUnit.SECONDS) {
                            @Override
                            protected Object loadValue() {
                                return metricsMap.get(metricsEntry.getKey());
                            }
                        }
                );
            }

        } catch (Exception e) {
            logger.error("Unable to publish state metrics for GA", e);
        }
    }

    @Override
    public String toString() {
        return "GoogleAnalyticsV4DataFormatter{" +
                "dataFormatterConfig=" + dataFormatterConfig +
                '}';
    }
}
