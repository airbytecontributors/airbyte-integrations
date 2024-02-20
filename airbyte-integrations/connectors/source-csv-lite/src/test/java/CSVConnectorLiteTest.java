import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.airbyte.integrations.source.csv.CSVConnectorLite;
import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.UUID;

public class CSVConnectorLiteTest {

    static JsonNode config;
    static ConfiguredAirbyteCatalog catalog;
    private static AuthInfo authInfo;
    private static EventSourceInfo eventSourceInfo;
    private static BicycleConfig bicycleConfig;
    private static CSVConnectorLite csvConnector;

    @BeforeAll
    public static void setupBicycleConsumer() {
        String serverURL =  "https://api.dev.bicycle.io";
        String metricStoreURL =  "http://anom-metric-store.bha.svc.cluster.local:4242/api/anoms/api/put?details";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJST0xFIjoiQVBJIiwic3ViIjoiUmF2aSIsIk9SR19JRCI6IjgwIiwiaXNzIjoic3VtaXRAYmljeWNsZS5pbyIsImlhdCI6MTY3OTk5NTMxMiwiVEVOQU5UIjoiZXZ0LWZiYjk2N2FkLTI1ZjItNGVlZS1iMmU1LWY1MmIwNDdiZTJlZiIsImp0aSI6ImYyMjJmYzAzLTdkNmMtNGRlYy1iN2EifQ.I99UmB_fvY7fDWuqjVo_G7itQtV9sGAMsml4gQkoSPc";
        String connectorId = "2278a5da-4692-44e0-b062-25716ac6ce09";
        String userId = "";
        String eventSourceType= "EVENT";
        String tenantId = "evt-fbb967ad-25f2-4eee-b2e5-f52b047be2ef";

        bicycleConfig = new BicycleConfig(serverURL, metricStoreURL,token, connectorId,uniqueIdentifier, tenantId, Mockito.mock(SystemAuthenticator.class),true);
        authInfo = bicycleConfig.getAuthInfo();
        eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);


        ObjectMapper mapper = new ObjectMapper();
        config = mapper.createObjectNode();
        //((ObjectNode)config).put("url", "");
        //((ObjectNode)config).put("url", "file:///home/ravi/Desktop/test1.csv");
        ((ObjectNode)config).put("url", "file:///home/ravi/Desktop/BJune2023Transactions.csv");
        ((ObjectNode)config).put("timeHeader", "SERVER_TIME_CREATED_AT");
        ((ObjectNode)config).put("timeFormat", "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]['Z']");
        ((ObjectNode)config).put("timeZone", "UTC");
        ((ObjectNode)config).put("datasetName", "test-csv");
        ((ObjectNode)config).put("format", "csv");
        ((ObjectNode)config).put("backfill", true);
        ((ObjectNode)config).put("replay", false);
        ((ObjectNode)config).put("backfillDateTime", "test");
        ((ObjectNode)config).put("backfillStartDateTime", "2023-06-01T05:30:00.000000Z");
        ((ObjectNode)config).put("backfillEndDateTime", "2023-07-01T05:29:59.000000Z");
        ((ObjectNode)config).put("publishEventsEnabled", "false");
        JsonNode config1 = mapper.createObjectNode();
        ((ObjectNode)config1).put("storage", "local");
        ((ObjectNode)config1).put("service_account_json", "");
        ((ObjectNode)config).put("provider", config1);
        catalog= new ConfiguredAirbyteCatalog();
        catalog.getAdditionalProperties().put("bicycleServerURL", serverURL);
        catalog.getAdditionalProperties().put("bicycleTenantId", tenantId);
        catalog.getAdditionalProperties().put("bicycleToken", token);
        catalog.getAdditionalProperties().put("bicycleConnectorId", connectorId);
        catalog.getAdditionalProperties().put("bicycleEventSourceType", eventSourceType);
        catalog.getAdditionalProperties().put("bicycleMetricStoreURL", metricStoreURL);
        catalog.getAdditionalProperties().put("isOnPrem", "true");

        String consumerThreadId = UUID.randomUUID().toString();
        ConnectorConfigManager mockConnectorConfigManager = Mockito.mock(ConnectorConfigManager.class);
        Mockito.when(mockConnectorConfigManager.getRuntimeConfig(Mockito.any(), Mockito.any())).thenReturn(null);
        csvConnector = new CSVConnectorLite(Mockito.mock(SystemAuthenticator.class),
                                            Mockito.mock(EventConnectorJobStatusNotifier.class),
                                            null);
        csvConnector.setBicycleEventProcessorAndPublisher(bicycleConfig);
    }

    @Test
    public void testSyncData() throws Exception {
        csvConnector.syncData(config, catalog, new ObjectMapper().createObjectNode(), null);
        Assertions.assertTrue(true);
    }

}
