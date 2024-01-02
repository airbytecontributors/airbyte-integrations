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
        String serviceAccountJson = "{   \"type\": \"service_account\",   \"project_id\": \"customer-titanbrands\",   \"private_key_id\": \"04f4b88ec4893ceeeb820e7ed9964266455e52e0\",   \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC5AmXuY7fEIVZB\\nR6tnLb+CvC8xAecRX8de81pga0D97U9WTQ5fbSfiBBBEhCcN66vbIghMvO7fGUUo\\n+Gh6F6i0bzAd681vN9m8T/54WdmASsQgXsOox7ihXtsDlW337nWu7kbD3GZuB3qL\\nRW53pt0yB2gN4UR6FZ0kMsn8rHS6c0qX7qrcOcfOfcW8IkdaVEPlvpM81ROxiR/J\\nPNctE8YZ9/pJWRLZK8t99JKCYV5UH40UUU35J9eSUEOiMKdEg/dQyIyuOOPxlSj5\\nwjyKoYn1sasRs/kz8JRDG1ZgQcES37YqLUj1MnEkAdaFBGB3AWYMZOGhKue54Cep\\noDujbJaLAgMBAAECggEABJuPrrrWP9DViIJHClnN7dNoy99225LqfaEX77po5ioV\\neMCQinkTrjUls18kzdvAIV2dBIEMF4uJXzZyLuBAXl+kIvEIrviehQPraLnsIp4G\\n4AhPXSu9ztJBW1R+AAr07JD/CC+8CVXivzpqc7M4yd8vWaRYagxejoei48nm8+6S\\nTBXNfmSsOAAnpT4hmvlaM9WuLYP+DWMmgIqOWeSlqE0TV2EEqZXtV3NMTRc65lB3\\njpVISt+PtcSbo5OiTV2cQBrH3/bAtqC3fN2XmKkj6/iRpZvkX7bwod0dOneY0dqq\\nENNP3lrvXmy9PDmsyhe3cFcozeQfPJp83764WlwuKQKBgQDfAAxHAqIHcE8yMIMG\\nBcGu3MgxO9ZnceRG7sFO+lBxQNjj0ge57E9Aim6ws4795SPBeXWsX1p+QmHo8gvA\\nWMqNwJsKfi5slxf0Uw0McpwKAeNR0M2Fy3b+mcZ1HQZE698FCpZBT+VcMNylk62w\\ngpGkgp/JUa2icnTP5YPvLs0jLQKBgQDUYyNLkjv2+CTINr31Jo+zLq+c8kGWWz97\\nE/jl85464gpovNZQFCMCH99CVMblsk5YuY/NHbsANG7/B/cPWuiepwJhOlNS1+V9\\nDY2fSaAWaRxcHI2uGRAZmP8ykr+AkTw+BH2rUStvZF6OHQSX1l5jdFhpO901GrsR\\n5x5ynmKTlwKBgQDdhEGt5Eqp0wGInLH6sIs2NXDTn1oaxgL6Bz+VQYfZCI6quD8e\\njEQpm5nbA3LeeNjs3sdqpAnTdAOoj4/m0QzchOA+KxvYmLqd5EYJAGxKcJer9RvD\\ndVrODFkciuloW1nduyeI12HCE3OLMsiNlxYv8eXh6LXvsXyh658EYFw8PQKBgFpT\\nT090gcwm/H2pyl/YZoTt9gHphbtMU1Mky1YApeqk94hTx1GHPtxIccrkAzLtAiT5\\nxg6V9lG/+vS7jncZUpUmsfxnscgpyY/Fk9WKmmrtiQIjop3ISvCYAPChOJCVM+ms\\nP0X831wgc5Y9ARcSNFJXyMH4k7tiAu06PdjBQ2tBAoGALrvvKhzcgKqRsP2l7no+\\nq9dsoDW+KI66H+Mg5u6iZHOTejd8EF0RvSOqSDFcZudR9AK7BHGLw7zhq10GjySO\\nGx16UNfIVwcJxnZbNoftCmKR2tvfOpxg0rbdyXYl/8x2rmdinAhaPURbbAYrufpN\\niSm3v8anu4+RdvYs1V+xsgw=\\n-----END PRIVATE KEY-----\\n\",   \"client_email\": \"bicycle-read-events@customer-titanbrands.iam.gserviceaccount.com\",   \"client_id\": \"115507561198979186445\",   \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",   \"token_uri\": \"https://oauth2.googleapis.com/token\",   \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",   \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/bicycle-read-events%40customer-titanbrands.iam.gserviceaccount.com\",   \"universe_domain\": \"googleapis.com\" }";

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
