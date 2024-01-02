package io.airbyte.integrations.source.event.bigquery;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author sumitmaheshwari
 * Created on 12/10/2023
 */
public class TestRowCount {

    public static void main(String[] args) throws InterruptedException, IOException {
        String projectId = "customer-titanbrands";

        // Set your BigQuery dataset and table name
        String datasetName = "analytics_308033850";
        String tableName = "events_intraday_20231225";
        String serviceAccountJson = "";

        // Initialize the BigQuery client
      //  BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(serviceAccountJson.getBytes(
                StandardCharsets.UTF_8)));

        // Step 1: Initialize BigQuery service
        // Here we set our project ID and get the `BigQuery` service object
        // this is the interface to our BigQuery instance that
        // we use to execute jobs on
        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId)
                .setCredentials(credentials)
                .build().getService();

        // Construct the SQL query to count records in the table
        //String query = "SELECT COUNT(*) AS record_count FROM `" + projectId + "." + datasetName + "." + tableName + "`";

        String query = "SELECT MAX(event_timestamp) AS record_count FROM `" + projectId + "." + datasetName + "." + tableName + "`";


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
