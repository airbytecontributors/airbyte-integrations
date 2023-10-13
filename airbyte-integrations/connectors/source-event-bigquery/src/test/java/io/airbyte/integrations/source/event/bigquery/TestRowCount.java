package io.airbyte.integrations.source.event.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

/**
 * @author sumitmaheshwari
 * Created on 12/10/2023
 */
public class TestRowCount {

    public static void main(String[] args) throws InterruptedException {
        String projectId = "customer-titanbrands";

        // Set your BigQuery dataset and table name
        String datasetName = "analytics_307818505";
        String tableName = "events_20231009";

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
            System.out.println("Record Count: " + recordCount);
        }
    }


}
