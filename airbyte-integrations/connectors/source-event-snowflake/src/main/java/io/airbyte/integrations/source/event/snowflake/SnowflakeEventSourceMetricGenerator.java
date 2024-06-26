package io.airbyte.integrations.source.event.snowflake;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import io.airbyte.integrations.bicycle.base.integration.MetricAsEventsGenerator;
import io.bicycle.event.publisher.api.BicycleEventPublisher;
import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeEventSourceMetricGenerator extends MetricAsEventsGenerator {
    private static final String TABLE_ROWS_METRIC = "bigquery_event_table_row_count";
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeEventSourceMetricGenerator.class);
    private final SnowflakeEventSourceConfig snowflakeEventSourceConfig;

    public SnowflakeEventSourceMetricGenerator(BicycleConfig bicycleConfig, EventSourceInfo eventSourceInfo,
                                               JsonNode config, BicycleEventPublisher bicycleEventPublisher,
                                               SnowflakeEventSource snowFlakeEventSource,
                                               SnowflakeEventSourceConfig snowflakeEventSourceConfig) {
        super(bicycleConfig, eventSourceInfo, config, bicycleEventPublisher, snowFlakeEventSource);
        this.snowflakeEventSourceConfig = snowflakeEventSourceConfig;
    }

    private Map<String, Long> getNumberOfRowsMetric() {
        Map<String, Long> streamNameToCountMap = new HashMap<>();
/*        try {
            String projectId = snowflakeEventSourceConfig.getProjectId();
            // Set your BigQuery dataset and table name
            String datasetName = snowflakeEventSourceConfig.getDatasetId();
            List<String> tableNames = snowflakeStreamGetter.getStreamNames();
            BigQuery bigquery = createAuthorizedClient(snowflakeEventSourceConfig.getCredentialsJson());


            for (String tableName : tableNames) {
                try {
                    TableId tableId = TableId.of(projectId, datasetName, tableName);

                    // Get the table metadata
                    Table table = bigquery.getTable(tableId);

                    // Retrieve the row count from the table metadata
                    BigInteger rowCount = table.getNumRows();
                    streamNameToCountMap.put(tableName, rowCount.longValue());
                } catch (Exception e) {
                    LOGGER.error("Unable to get row count for big query for connector {} for table name {}, " +
                                    "dataset name {} because of {}", eventSourceInfo.getEventSourceId(), tableName,
                            datasetName, e);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Unable to get row count for big query for connector {} because of {}",
                    eventSourceInfo.getEventSourceId(), e);
        }*/

        return streamNameToCountMap;
    }

    public static BigQuery createAuthorizedClient(String credentialJson) {
        try {
            // Create a CredentialsProvider using the environment variable
            ServiceAccountCredentials serviceAccountCredentials =
                    ServiceAccountCredentials.fromStream(new ByteArrayInputStream(credentialJson.
                            getBytes(Charset.defaultCharset())));

            return BigQueryOptions.newBuilder()
                    .setCredentials(serviceAccountCredentials)
                    .build().getService();
        } catch (Exception e) {
            throw new RuntimeException("Error creating authorized BigQuery client: " + e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            logger.info("Starting the metrics collection for Big Query Event Source Connector");
            Map<String, String> attributes = new HashMap<>();
            attributes.put(UNIQUE_IDENTIFIER, bicycleConfig.getUniqueIdentifier());
            attributes.put(CONNECTOR_ID, eventSourceInfo.getEventSourceId());

            Map<String, Long> tableNameToCountMetric = getNumberOfRowsMetric();
            for (Map.Entry<String, Long> entry: tableNameToCountMetric.entrySet()) {
                Map<String, String> tags = new HashMap<>();
                tags.put("streamName", entry.getKey());
                metricsMap.put(getTagEncodedMetricName(TABLE_ROWS_METRIC, tags), entry.getValue());
                attributes.put(TABLE_ROWS_METRIC + "_" + entry.getKey(), String.valueOf(entry.getValue()));
            }

            int totalRecordsConsumed = ((SnowflakeEventSource) eventConnector).getTotalRecordsConsumed();
            attributes.put(TOTAL_EVENTS_PROCESSED_METRIC, String.valueOf(totalRecordsConsumed));
            metricsMap.put(getTagEncodedMetricName(TOTAL_EVENTS_PROCESSED_METRIC, new HashMap<>()),
                    Long.valueOf(totalRecordsConsumed));

            this.publishMetrics(attributes, metricsMap);
        } catch (Exception exception) {
            logger.error("Unable to publish metrics for Big Query Event Source Connector", exception);
        }
    }
}
