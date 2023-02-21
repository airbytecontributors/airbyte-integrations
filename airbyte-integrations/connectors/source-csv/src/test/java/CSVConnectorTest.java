import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import io.airbyte.integrations.bicycle.base.integration.BicycleConfig;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.airbyte.integrations.source.csv.CSVConnector;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class CSVConnectorTest {

    static JsonNode config;
    static ConfiguredAirbyteCatalog catalog;
    private static AuthInfo authInfo;
    private static EventSourceInfo eventSourceInfo;
    private static BicycleConfig bicycleConfig;
    private static CSVConnector csvConnector;

    @BeforeAll
    public static void setupBicycleConsumer() {
        String serverURL =  "https://api.dev.bicycle.io";
        String metricStoreURL =  "http://anom-metric-store.bha.svc.cluster.local:4242/api/anoms/api/put?details";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJSYXZpIiwiT1JHX0lEIjoiMzgiLCJpc3MiOiJhZG1pbiIsImlhdCI6MTYzMjQ2NTIxOSwiVEVOQU5UIjoiZHRzLTU2OGRjZWEzLTExYjAtNDBjMi1iODllLWQxODlmZTc3MDAyZSIsImp0aSI6ImViYTY5ZDU0LTViMTItNDYyZi1iODkifQ.ucz5kNwT2NfORTf5VDMuMrfPBLqa3xLy34iWOlwNZqk";
        String connectorId = "c_connector_stream:950ae7a5-d88d-413b-a96b-d61348aaf4a1";
        String userId = "";
        String eventSourceType= "EVENT";
        String tenantId = "";

        Map<String, Long> totalRecordsRead = null;
        bicycleConfig = new BicycleConfig(serverURL, metricStoreURL,token, connectorId,uniqueIdentifier, tenantId, Mockito.mock(SystemAuthenticator.class),true);
        authInfo = bicycleConfig.getAuthInfo();
        eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);


        ObjectMapper mapper = new ObjectMapper();
        config = mapper.createObjectNode();
        //((ObjectNode)config).put("url", "");
        ((ObjectNode)config).put("url", "gs://kdev-repo-1645/test.csv");
        ((ObjectNode)config).put("timeHeader", "transactiondate");
        ((ObjectNode)config).put("timeFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS");
        ((ObjectNode)config).put("timeZone", "UTC");
        ((ObjectNode)config).put("datasetName", "test-csv");
        ((ObjectNode)config).put("format", "csv");
        ((ObjectNode)config).put("backfill", true);
        ((ObjectNode)config).put("replay", false);
        ((ObjectNode)config).put("backfillDateTime", "test");
        ((ObjectNode)config).put("backfillStartDateTime", "2022-12-27T00:00:00.000");
        ((ObjectNode)config).put("backfillEndDateTime", "2023-01-15T00:00:00.000");
        JsonNode config1 = mapper.createObjectNode();
        ((ObjectNode)config1).put("storage", "GCS");
        ((ObjectNode)config1).put("service_account_json", "");
        ((ObjectNode)config).put("provider", config1);
        catalog= new ConfiguredAirbyteCatalog();
        catalog.getAdditionalProperties().put("bicycleServerURL", serverURL);
        catalog.getAdditionalProperties().put("bicycleTenantId", "dts-568dcea3-11b0-40c2-b89e-d189fe77002e");
        catalog.getAdditionalProperties().put("bicycleToken", token);
        catalog.getAdditionalProperties().put("bicycleConnectorId", connectorId);
        catalog.getAdditionalProperties().put("bicycleEventSourceType", eventSourceType);
        catalog.getAdditionalProperties().put("bicycleMetricStoreURL", metricStoreURL);
        catalog.getAdditionalProperties().put("isOnPrem", "true");

        String consumerThreadId = UUID.randomUUID().toString();
        csvConnector = new CSVConnector(Mockito.mock(SystemAuthenticator.class), Mockito.mock(EventConnectorJobStatusNotifier.class));
        csvConnector.setBicycleEventProcessorAndPublisher(bicycleConfig);
    }

    @Test
    public void testReadFile() throws Exception {
        csvConnector.read(config, catalog, null);
        Assertions.assertTrue(true);
    }

    @Test
    public void testPublishEvents() throws Exception {
        try {
            csvConnector.read(config, catalog, new ObjectMapper().createObjectNode());
            Assertions.assertTrue(true);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail();
        }
    }
}
