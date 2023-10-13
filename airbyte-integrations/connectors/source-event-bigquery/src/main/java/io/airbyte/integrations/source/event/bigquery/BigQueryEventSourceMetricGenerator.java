package io.airbyte.integrations.source.event.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import io.airbyte.integrations.bicycle.base.integration.MetricAsEventsGenerator;
import io.bicycle.event.publisher.api.BicycleEventPublisher;
import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
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

            // Initialize the BigQuery client
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            // Construct the SQL query to count records in the table
            String query = "SELECT COUNT(*) AS record_count FROM `" + projectId + "." + datasetName + "." + tableName + "`";

            // Create a query configuration
            QueryJobConfiguration.Builder queryConfigBuilder = QueryJobConfiguration.newBuilder(query)
                    .setPriority(QueryJobConfiguration.Priority.BATCH)
                    .setDefaultDataset(datasetName);

            // Run the query
            TableResult result = bigquery.query(queryConfigBuilder.build());

            // Extract the record count
            for (FieldValueList row : result.iterateAll()) {
                long recordCount = row.get("record_count").getLongValue();
                 return recordCount;
            }
        } catch (Exception e) {
            LOGGER.error("Unable to get row count for big query for connector {} because of {}",
                    eventSourceInfo.getEventSourceId(), e);
        }

        return -1;
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
