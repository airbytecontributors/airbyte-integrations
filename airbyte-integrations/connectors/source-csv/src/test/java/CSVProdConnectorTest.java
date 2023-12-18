import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.airbyte.integrations.source.csv.CSVConnector;
import io.bicycle.airbyte.integrations.source.csv.CSVProdConnector;
import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.integration.connector.runtime.RuntimeConfig;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * @author sumitmaheshwari
 * Created on 10/12/2023
 */
public class CSVProdConnectorTest {
    private static final String GCS_ZIP_URL = "gs://inception-general-purpose-bucket/test-csv/oct.zip";
    private static final String GCS_FILE_URL = "gs://inception-general-purpose-bucket/sumup/TransactionStatus-Allmonths.csv";
    private static final String TOKEN = "";
    private static final String GCS_SERVICE_TOKEN = "";
    private static final String PUBLIC_ZIP_FILE = "";
    private static final String PUBLIC_FILE = "https://storage.googleapis.com/kdev-blob-store/evt-fbb967ad-25f2-4eee-b2e5-f52b047be2ef/test-namespace/test-path/1ec89429-8516-43c5-8d2b-6cd22b63f248?GoogleAccessId=alert-store-bucket-access@pivotal-canto-171605.iam.gserviceaccount.com&Expires=1703045130&Signature=Lzkc2qVoaeBlE1S%2FHrhCr3Cc5HTVB0T5XJLDew9kpMniAPyI6DtfGf8RSs1baUWj6MDo%2B96JG7jXK4u07fpRTgT0MZeCmgmTHJwZOqLINtAEU2lzAluxQqNa6iovr5YVe9t0GuCq8jNX31Itws1m1H7aPCK%2B75M7RcrOZ8JrnWq8fGSR3FEu1wm6tC30fkrmT0fdEUXKNR2iK8KkUOy789ODs836vcJoW%2BSNcJKEUZC5%2FSznIWpJkZY6CiZPmLTodOZNJeiFtJ6gmvBr4coAPsk2vkXLRqAnno3sNR%2FEcxv2F5rBdh6hE8fR%2FkzVmIp1gasPE14KaTc8U1rG3wPHdA%3D%3D";

   // @Test
    public void testCSVProdConnectorWithPublicUrl() throws IOException {

        String serverURL =  "https://api.dev.bicycle.io";
        String metricStoreURL =  "http://anom-metric-store.bha.svc.cluster.local:4242/api/anoms/api/put?details";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String connectorId = "8659da17-eb4b-417d-a762-9d59a85a9eef";
        String userId = "";
        String eventSourceType= "EVENT";
        String tenantId = "";

        BicycleConfig bicycleConfig = new BicycleConfig(serverURL, metricStoreURL, TOKEN, connectorId,uniqueIdentifier,
                tenantId, Mockito.mock(SystemAuthenticator.class),true);

        JsonNode config = getPublicUrlConfig();
        String fileUrl = config.get("url").asText();
        String dateTimePattern = config.get("timeFormat").asText();
        String dateTimeField = config.get("timeHeader").asText();
        ConnectorConfigManager connectorConfigManager = Mockito.mock(ConnectorConfigManager.class);
        Mockito.when(connectorConfigManager.getRuntimeConfig(Mockito.any(AuthInfo.class), Mockito.any(String.class))).
                thenReturn(RuntimeConfig.getDefaultInstance());
        CSVConnector csvConnector = new CSVConnector(Mockito.mock(SystemAuthenticator.class),
                Mockito.mock(EventConnectorJobStatusNotifier.class), connectorConfigManager);
        csvConnector.setBicycleEventProcessorAndPublisher(bicycleConfig);


        CSVProdConnector csvProdConnector = new CSVProdConnector(fileUrl, dateTimePattern, "UTC",
                dateTimeField, "backfill-job-1", bicycleConfig.getConnectorId(),
                eventSourceType,  csvConnector, 10, 5000, config);


        csvProdConnector.doRead();

    }

  //  @Test
    public void testCSVProdConnectorWithPublicUrlAndZipFile() throws IOException {

        String serverURL =  "https://api.dev.bicycle.io";
        String metricStoreURL =  "http://anom-metric-store.bha.svc.cluster.local:4242/api/anoms/api/put?details";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String connectorId = "8659da17-eb4b-417d-a762-9d59a85a9eef";
        String eventSourceType= "EVENT";
        String tenantId = "";

        BicycleConfig bicycleConfig = new BicycleConfig(serverURL, metricStoreURL, TOKEN, connectorId,uniqueIdentifier,
                tenantId, Mockito.mock(SystemAuthenticator.class),true);

        JsonNode config = getPublicUrlConfig();
        String fileUrl = config.get("url").asText();
        String dateTimePattern = config.get("timeFormat").asText();
        String dateTimeField = config.get("timeHeader").asText();
        ConnectorConfigManager connectorConfigManager = Mockito.mock(ConnectorConfigManager.class);
        Mockito.when(connectorConfigManager.getRuntimeConfig(Mockito.any(AuthInfo.class), Mockito.any(String.class))).
                thenReturn(RuntimeConfig.getDefaultInstance());
        CSVConnector csvConnector = new CSVConnector(Mockito.mock(SystemAuthenticator.class),
                Mockito.mock(EventConnectorJobStatusNotifier.class), connectorConfigManager);
        csvConnector.setBicycleEventProcessorAndPublisher(bicycleConfig);

        CSVProdConnector csvProdConnector = new CSVProdConnector(fileUrl, dateTimePattern, dateTimeField,
                "UTC", "backfill-job-1", bicycleConfig.getConnectorId(),
                eventSourceType,  csvConnector, 10, 5000, config);


        csvProdConnector.doRead();

    }

   // @Test
    public void testCSVProdConnectorWithGCS() throws IOException {

        String serverURL =  "https://api.dev.bicycle.io";
        String metricStoreURL =  "http://anom-metric-store.bha.svc.cluster.local:4242/api/anoms/api/put?details";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String connectorId = "8659da17-eb4b-417d-a762-9d59a85a9eef";
        String userId = "";
        String eventSourceType= "EVENT";
        String tenantId = "";

        BicycleConfig bicycleConfig = new BicycleConfig(serverURL, metricStoreURL, TOKEN, connectorId,uniqueIdentifier,
                tenantId, Mockito.mock(SystemAuthenticator.class),true);

        JsonNode config = getGSCConfig(GCS_FILE_URL, "CREATED_AT",
                "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]['Z']");
        String fileUrl = config.get("url").asText();
        String dateTimePattern = config.get("timeFormat").asText();
        String dateTimeField = config.get("timeHeader").asText();
        ConnectorConfigManager connectorConfigManager = Mockito.mock(ConnectorConfigManager.class);
        Mockito.when(connectorConfigManager.getRuntimeConfig(Mockito.any(AuthInfo.class), Mockito.any(String.class))).
                thenReturn(RuntimeConfig.getDefaultInstance());
        CSVConnector csvConnector = new CSVConnector(Mockito.mock(SystemAuthenticator.class),
                Mockito.mock(EventConnectorJobStatusNotifier.class), connectorConfigManager);
        csvConnector.setBicycleEventProcessorAndPublisher(bicycleConfig);

        CSVProdConnector csvProdConnector = new CSVProdConnector(fileUrl, dateTimePattern, "UTC", dateTimeField,
                "backfill-job-1", bicycleConfig.getConnectorId(), eventSourceType,  csvConnector,
                10, 5000, config);


        csvProdConnector.doRead();

    }

  //  @Test
    public void testCSVProdConnectorWithGCSSumup() throws IOException {

        String serverURL =  "https://api.dev.bicycle.io";
        String metricStoreURL =  "http://anom-metric-store.bha.svc.cluster.local:4242/api/anoms/api/put?details";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String connectorId = "8659da17-eb4b-417d-a762-9d59a85a9eef";
        String userId = "";
        String eventSourceType= "EVENT";
        String tenantId = "";

        BicycleConfig bicycleConfig = new BicycleConfig(serverURL, metricStoreURL, TOKEN, connectorId,uniqueIdentifier,
                tenantId, Mockito.mock(SystemAuthenticator.class),true);

        JsonNode config = getGSCConfigWithBackfillStartAndEndTime("gs://inception-general-purpose-bucket/sumup/AllTransactions-Modified.csv", "SERVER_TIME_CREATED_AT",
                "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]['Z']");
        String fileUrl = config.get("url").asText();
        String dateTimePattern = config.get("timeFormat").asText();
        String dateTimeField = config.get("timeHeader").asText();
        ConnectorConfigManager connectorConfigManager = Mockito.mock(ConnectorConfigManager.class);
        Mockito.when(connectorConfigManager.getRuntimeConfig(Mockito.any(AuthInfo.class), Mockito.any(String.class))).
                thenReturn(RuntimeConfig.getDefaultInstance());
        CSVConnector csvConnector = new CSVConnector(Mockito.mock(SystemAuthenticator.class),
                Mockito.mock(EventConnectorJobStatusNotifier.class), connectorConfigManager);
        csvConnector.setBicycleEventProcessorAndPublisher(bicycleConfig);

        CSVProdConnector csvProdConnector = new CSVProdConnector(fileUrl, dateTimePattern, "UTC", dateTimeField,
                "backfill-job-1", bicycleConfig.getConnectorId(), eventSourceType,  csvConnector,
                10, 5000, config);


        csvProdConnector.doRead();

    }


 //   @Test
    public void testCSVProdConnectorWithGCSAndZipFile() throws IOException {

        String serverURL =  "https://api.dev.bicycle.io";
        String metricStoreURL =  "http://anom-metric-store.bha.svc.cluster.local:4242/api/anoms/api/put?details";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String connectorId = "7dd1b74c-034e-45b8-84c0-5d718ec793e6";
        String userId = "";
        String eventSourceType= "EVENT";
        String tenantId = "";

        BicycleConfig bicycleConfig = new BicycleConfig(serverURL, metricStoreURL, TOKEN, connectorId,uniqueIdentifier,
                tenantId, Mockito.mock(SystemAuthenticator.class),true);

        JsonNode config = getGSCConfig(GCS_ZIP_URL, "booking_timestamp", "yyyy-MM-dd HH:mm:ss.[SSSSSS][SSSSS][SSSS][SSS][SS][S] z");
        String fileUrl = config.get("url").asText();
        String dateTimePattern = config.get("timeFormat").asText();
        String dateTimeField = config.get("timeHeader").asText();
        ConnectorConfigManager connectorConfigManager = Mockito.mock(ConnectorConfigManager.class);
        Mockito.when(connectorConfigManager.getRuntimeConfig(Mockito.any(AuthInfo.class), Mockito.any(String.class))).
                thenReturn(RuntimeConfig.getDefaultInstance());
        CSVConnector csvConnector = new CSVConnector(Mockito.mock(SystemAuthenticator.class),
                Mockito.mock(EventConnectorJobStatusNotifier.class), connectorConfigManager);
        csvConnector.setBicycleEventProcessorAndPublisher(bicycleConfig);

        CSVProdConnector csvProdConnector = new CSVProdConnector(fileUrl, dateTimePattern, "UTC", dateTimeField,
                "backfill-job-1", bicycleConfig.getConnectorId(), eventSourceType,  csvConnector,
                10, 5000, config);


        csvProdConnector.doRead();

    }

    public void testCSVProdConnectorWithGCSAndZipFileAndBackfillStartAndEndTime() throws IOException {

        String serverURL =  "https://api.dev.bicycle.io";
        String metricStoreURL =  "http://anom-metric-store.bha.svc.cluster.local:4242/api/anoms/api/put?details";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String connectorId = "8659da17-eb4b-417d-a762-9d59a85a9eef";
        String userId = "";
        String eventSourceType= "EVENT";
        String tenantId = "";

        BicycleConfig bicycleConfig = new BicycleConfig(serverURL, metricStoreURL, TOKEN, connectorId,uniqueIdentifier,
                tenantId, Mockito.mock(SystemAuthenticator.class),true);

        JsonNode config = getGSCConfigWithBackfillStartAndEndTime(GCS_FILE_URL,
                "booking_timestamp", "yyyy-MM-dd HH:mm:ss.[SSSSSS][SSSSS][SSSS][SSS][SS][S] z");

        String fileUrl = config.get("url").asText();
        String dateTimePattern = config.get("timeFormat").asText();
        String dateTimeField = config.get("timeHeader").asText();
        CSVConnector csvConnector = new CSVConnector(Mockito.mock(SystemAuthenticator.class),
                Mockito.mock(EventConnectorJobStatusNotifier.class), null);
        csvConnector.setBicycleEventProcessorAndPublisher(bicycleConfig);

        CSVProdConnector csvProdConnector = new CSVProdConnector(fileUrl, dateTimePattern, "UTC", dateTimeField,
                "backfill-job-1", bicycleConfig.getConnectorId(), eventSourceType,  csvConnector,
                10, 5000, config);


        csvProdConnector.doRead();

    }

  //  @Test
    public void testCSVProdConnectorWithGCSAndWithBackfillDisabled() throws IOException {

        String serverURL =  "https://api.dev.bicycle.io";
        String metricStoreURL =  "http://anom-metric-store.bha.svc.cluster.local:4242/api/anoms/api/put?details";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String connectorId = "8659da17-eb4b-417d-a762-9d59a85a9eef";
        String eventSourceType= "EVENT";
        String tenantId = "";

        BicycleConfig bicycleConfig = new BicycleConfig(serverURL, metricStoreURL, TOKEN, connectorId,uniqueIdentifier,
                tenantId, Mockito.mock(SystemAuthenticator.class),true);

        JsonNode config = getGSCConfigWithBackfillDisabled(GCS_FILE_URL,
                "CREATED_AT", "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]['Z']");

        String fileUrl = config.get("url").asText();
        String dateTimePattern = config.get("timeFormat").asText();
        String dateTimeField = config.get("timeHeader").asText();
        ConnectorConfigManager connectorConfigManager = Mockito.mock(ConnectorConfigManager.class);
        Mockito.when(connectorConfigManager.getRuntimeConfig(Mockito.any(AuthInfo.class), Mockito.any(String.class))).
                thenReturn(RuntimeConfig.getDefaultInstance());
        CSVConnector csvConnector = new CSVConnector(Mockito.mock(SystemAuthenticator.class),
                Mockito.mock(EventConnectorJobStatusNotifier.class), connectorConfigManager);
        csvConnector.setBicycleEventProcessorAndPublisher(bicycleConfig);

        CSVProdConnector csvProdConnector = new CSVProdConnector(fileUrl, dateTimePattern, "UTC", dateTimeField,
                "backfill-job-1", bicycleConfig.getConnectorId(), eventSourceType,  csvConnector,
                10, 5000, config);


        csvProdConnector.doRead();

    }

 //   @Test
    public void testCSVProdConnectorPreviewWithGCS() {

        String serverURL =  "https://api.dev.bicycle.io";
        String metricStoreURL =  "http://anom-metric-store.bha.svc.cluster.local:4242/api/anoms/api/put?details";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String connectorId = "8659da17-eb4b-417d-a762-9d59a85a9eef";
        String eventSourceType= "EVENT";
        String tenantId = "";

        BicycleConfig bicycleConfig = new BicycleConfig(serverURL, metricStoreURL, TOKEN, connectorId, uniqueIdentifier,
                tenantId, Mockito.mock(SystemAuthenticator.class),false);

        JsonNode config = getGSCConfigWithBackfillDisabled(GCS_ZIP_URL,
                "booking_timestamp",
                "yyyy-MM-dd HH:mm:ss.[SSSSSS][SSSSS][SSSS][SSS][SS][S] z");

        ConnectorConfigManager connectorConfigManager = Mockito.mock(ConnectorConfigManager.class);
        Mockito.when(connectorConfigManager.getRuntimeConfig(Mockito.any(AuthInfo.class), Mockito.any(String.class))).
                thenReturn(RuntimeConfig.getDefaultInstance());
        CSVConnector csvConnector = new CSVConnector(Mockito.mock(SystemAuthenticator.class),
                Mockito.mock(EventConnectorJobStatusNotifier.class), connectorConfigManager);
        csvConnector.setBicycleEventProcessorAndPublisher(bicycleConfig);

        try {
            csvConnector.preview(config, getConfiguredAirbyteCatalog(), null);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

    }

  //  @Test
    public void testCSVProdConnectorCheckWithGCS() {

        String serverURL =  "https://api.dev.bicycle.io";
        String metricStoreURL =  "http://anom-metric-store.bha.svc.cluster.local:4242/api/anoms/api/put?details";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String connectorId = "8659da17-eb4b-417d-a762-9d59a85a9eef";
        String eventSourceType= "EVENT";
        String tenantId = "";

        BicycleConfig bicycleConfig = new BicycleConfig(serverURL, metricStoreURL, TOKEN, connectorId, uniqueIdentifier,
                tenantId, Mockito.mock(SystemAuthenticator.class),false);

        JsonNode config = getGSCConfigWithBackfillDisabled(GCS_ZIP_URL,
                "booking_timestamp",
                "yyyy-MM-dd HH:mm:ss.[SSSSSS][SSSSS][SSSS][SSS][SS][S] z");

        ConnectorConfigManager connectorConfigManager = Mockito.mock(ConnectorConfigManager.class);
        Mockito.when(connectorConfigManager.getRuntimeConfig(Mockito.any(AuthInfo.class), Mockito.any(String.class))).
                thenReturn(RuntimeConfig.getDefaultInstance());
        CSVConnector csvConnector = new CSVConnector(Mockito.mock(SystemAuthenticator.class),
                Mockito.mock(EventConnectorJobStatusNotifier.class), connectorConfigManager);
        csvConnector.setBicycleEventProcessorAndPublisher(bicycleConfig);

        try {
            csvConnector.check(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private JsonNode getPublicUrlConfig() {

        ObjectMapper mapper = new ObjectMapper();
        JsonNode config = mapper.createObjectNode();
        //((ObjectNode)config).put("url", "");
        //((ObjectNode)config).put("url", "file:///home/ravi/Desktop/test1.csv");
        ((ObjectNode) config).put("url", PUBLIC_FILE);
        ((ObjectNode) config).put("timeHeader", "Start Time");
        ((ObjectNode) config).put("timeFormat", "yyyy-MMM-dd HH:mm:ss z");
        ((ObjectNode) config).put("timeZone", "UTC");
        ((ObjectNode) config).put("datasetName", "test-csv");
        ((ObjectNode) config).put("format", "csv");
        ((ObjectNode) config).put("backfill", false);
        ((ObjectNode) config).put("replay", false);
        ((ObjectNode) config).put("mode", "prod");
        ((ObjectNode) config).put("backfillDateTime", "test");
       // ((ObjectNode) config).put("backfillStartDateTime", "2023-06-01T05:30:00.000000Z");
       // ((ObjectNode) config).put("backfillEndDateTime", "2023-07-01T05:29:59.000000Z");
        ((ObjectNode) config).put("publishEventsEnabled", "false");
        ((ObjectNode)config).put("storage", "publicUrl");

        JsonNode config1 = mapper.createObjectNode();
        ((ObjectNode)config1).put("storage", "publicUrl");
        ((ObjectNode)config1).put("service_account_json", "");
        ((ObjectNode)config).put("provider", config1);

        return config;

    }

    private ConfiguredAirbyteCatalog getConfiguredAirbyteCatalog() {

        String serverURL =  "https://api.dev.bicycle.io";
        String metricStoreURL =  "http://anom-metric-store.bha.svc.cluster.local:4242/api/anoms/api/put?details";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String connectorId = "c_connector_stream:950ae7a5-d88d-413b-a96b-d61348aaf4a1";
        String userId = "";
        String eventSourceType= "EVENT";
        String tenantId = "";

        ConfiguredAirbyteCatalog catalog= new ConfiguredAirbyteCatalog();
        catalog.getAdditionalProperties().put("bicycleServerURL", serverURL);
        catalog.getAdditionalProperties().put("bicycleTenantId", "dts-568dcea3-11b0-40c2-b89e-d189fe77002e");
        catalog.getAdditionalProperties().put("bicycleToken", TOKEN);
        catalog.getAdditionalProperties().put("bicycleConnectorId", connectorId);
        catalog.getAdditionalProperties().put("bicycleEventSourceType", eventSourceType);
        catalog.getAdditionalProperties().put("bicycleMetricStoreURL", metricStoreURL);

        return catalog;
    }

    private JsonNode getGSCConfigWithBackfillDisabled(String gcsUrl, String dateTimeFieldName, String dateTimePattern) {

        JsonNode config = getGSCConfig(gcsUrl, dateTimeFieldName, dateTimePattern);
        return config;
    }

    private JsonNode getGSCConfigWithBackfillStartAndEndTime(String gcsUrl, String dateTimeFieldName, String dateTimePattern) {

         JsonNode config = getGSCConfig(gcsUrl, dateTimeFieldName, dateTimePattern);

        ((ObjectNode) config).put("backfillStartDateTime", "2023-05-01T00:00:00.000000Z");
        ((ObjectNode) config).put("backfillEndDateTime", "2023-05-31T23:59:59.000000Z");
        ((ObjectNode) config).put("backfill", true);
        return config;
    }

    private JsonNode getGSCConfig(String gcsUrl, String dateTimeFieldName, String dateTimePattern) {

        ObjectMapper mapper = new ObjectMapper();
        JsonNode config = mapper.createObjectNode();
        //((ObjectNode)config).put("url", "");
        //((ObjectNode)config).put("url", "file:///home/ravi/Desktop/test1.csv");
        ((ObjectNode) config).put("url", gcsUrl);
        ((ObjectNode) config).put("timeHeader", dateTimeFieldName);
        ((ObjectNode) config).put("timeFormat", dateTimePattern);
        ((ObjectNode) config).put("timeZone", "UTC");
        ((ObjectNode) config).put("datasetName", "test-csv");
        ((ObjectNode) config).put("format", "csv");
        ((ObjectNode) config).put("backfill", false);
        ((ObjectNode) config).put("replay", false);
        ((ObjectNode) config).put("mode", "prod");
        ((ObjectNode) config).put("backfillDateTime", "test");
//        ((ObjectNode) config).put("backfillStartDateTime", "2023-06-01T05:30:00.000000Z");
//        ((ObjectNode) config).put("backfillEndDateTime", "2023-07-01T05:29:59.000000Z");
        ((ObjectNode) config).put("publishEventsEnabled", "false");

        JsonNode config1 = mapper.createObjectNode();
        ((ObjectNode)config1).put("storage", "GCS");
        ((ObjectNode)config1).put("service_account_json", GCS_SERVICE_TOKEN);
        ((ObjectNode)config).put("provider", config1);

        return config;
    }

}
