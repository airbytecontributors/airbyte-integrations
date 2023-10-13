package io.airbyte.integrations.source.event.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import io.airbyte.integrations.bicycle.base.integration.MetricAsEventsGenerator;
import io.bicycle.event.publisher.api.BicycleEventPublisher;
import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryEventSourceMetricGenerator extends MetricAsEventsGenerator {
    private static final String TABLE_ROWS_METRIC = "bigquery_event_table_row_count";
    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryEventSourceMetricGenerator.class);
    private final BigQueryEventSourceConfig bigQueryEventSourceConfig;
    public BigQueryEventSourceMetricGenerator(BicycleConfig bicycleConfig, EventSourceInfo eventSourceInfo, JsonNode config, BicycleEventPublisher bicycleEventPublisher,
                                              BigQueryEventSource bigQueryEventSource, String streamName) {
        super(bicycleConfig, eventSourceInfo, config, bicycleEventPublisher, bigQueryEventSource);
        bigQueryEventSourceConfig = new BigQueryEventSourceConfig(config, streamName);
    }

    private long getNumberOfRowsMetric() {
        try {
            String projectId = bigQueryEventSourceConfig.getProjectId();
            // Set your BigQuery dataset and table name
            String datasetName = bigQueryEventSourceConfig.getDatasetId();
            String tableName = bigQueryEventSourceConfig.getStreamName();

            BigQuery bigquery = createAuthorizedClient(bigQueryEventSourceConfig.getCredentialsJson());

            TableId tableId = TableId.of(projectId, datasetName, tableName);

            // Get the table metadata
            Table table = bigquery.getTable(tableId);

            // Retrieve the row count from the table metadata
            BigInteger rowCount = table.getNumRows();
            return rowCount.longValue();
        } catch (Exception e) {
            LOGGER.error("Unable to get row count for big query for connector {} because of {}",
                    eventSourceInfo.getEventSourceId(), e);
        }

        return -1;
    }

    public static BigQuery createAuthorizedClient(String credentialJson) {
        try {
            // Create a CredentialsProvider using the environment variable
            ServiceAccountCredentials serviceAccountCredentials =
                    ServiceAccountCredentials.fromStream(new ByteArrayInputStream(credentialJson.getBytes()));

            return BigQueryOptions.newBuilder()
                    .setCredentials(serviceAccountCredentials)
                    .build().getService();
        } catch (Exception e) {
            e.printStackTrace();
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

            long numberOfRowsMetric = getNumberOfRowsMetric();
            metricsMap.put(getTagEncodedMetricName(TABLE_ROWS_METRIC, new HashMap<>()), numberOfRowsMetric);
            attributes.put(TABLE_ROWS_METRIC, String.valueOf(numberOfRowsMetric));

            int totalRecordsConsumed = ((BigQueryEventSource)eventConnector).getTotalRecordsConsumed();
            attributes.put(TOTAL_EVENTS_PROCESSED_METRIC, String.valueOf(totalRecordsConsumed));
            metricsMap.put(getTagEncodedMetricName(TOTAL_EVENTS_PROCESSED_METRIC, new HashMap<>()),
                    Long.valueOf(totalRecordsConsumed));

            this.publishMetrics(attributes, metricsMap);
        } catch (Exception exception) {
            logger.error("Unable to publish metrics for Big Query Event Source Connector", exception);
        }
    }
}
