import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.auth.model.BearerTokenAuthInfo;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.bicycle.airbyte.integrations.source.csv.CSVConnector;
import io.bicycle.airbyte.integrations.source.csv.CSVProdConnector;
import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * @author sumitmaheshwari
 * Created on 10/12/2023
 */
public class CSVProdConnectorTest {


    @Test
    public void testCSVProdConnectorWithPublicUrl() {

        String serverURL =  "https://api.dev.bicycle.io";
        String metricStoreURL =  "http://anom-metric-store.bha.svc.cluster.local:4242/api/anoms/api/put?details";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJST0xFIjoiQVBJIiwic3ViIjoic3VtaXQtdGVzdCIsIk9SR19JRCI6IjY0IiwiaXNzIjoic3VtaXRAYmljeWNsZS5pbyIsImlhdCI6MTY0NDk0MTQxMywiVEVOQU5UIjoiZW10LWU5ZTRlZjZjLTYzYzQtNDkzMC1iMzMxLTJkZjNhZjFlNzg4ZSIsImp0aSI6IjBkZjU4ZmFkLTk0NzMtNDQ4OS1iNzMifQ.t8F2oEwEFej1xU2LknY2pLsbgUW3x5YED8trN9QYzDU";
        String connectorId = "8659da17-eb4b-417d-a762-9d59a85a9eef";
        String userId = "";
        String eventSourceType= "EVENT";
        String tenantId = "";

        BicycleConfig bicycleConfig = new BicycleConfig(serverURL, metricStoreURL, token, connectorId,uniqueIdentifier,
                tenantId, Mockito.mock(SystemAuthenticator.class),true);
        AuthInfo authInfo = bicycleConfig.getAuthInfo();
        EventSourceInfo eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);

        JsonNode config = getPublicUrlConfig();
        String fileUrl = config.get("url").asText();
        String dateTimePattern = config.get("timeFormat").asText();
        String dateTimeField = config.get("timeHeader").asText();
        CSVConnector csvConnector = new CSVConnector(Mockito.mock(SystemAuthenticator.class),
                Mockito.mock(EventConnectorJobStatusNotifier.class), null);
        csvConnector.setBicycleEventProcessorAndPublisher(bicycleConfig);

        CSVProdConnector csvProdConnector = new CSVProdConnector(fileUrl, dateTimePattern, dateTimeField,
                "UTC", "backfill-job-1", bicycleConfig.getConnectorId(),
                eventSourceType,  csvConnector, 10, 5000, config);


        csvProdConnector.doRead();

    }

    @Test
    public void testCSVProdConnectorWithGCS() {

        String serverURL =  "https://api.dev.bicycle.io";
        String metricStoreURL =  "http://anom-metric-store.bha.svc.cluster.local:4242/api/anoms/api/put?details";
        String uniqueIdentifier = UUID.randomUUID().toString();
        String token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJST0xFIjoiQVBJIiwic3ViIjoic3VtaXQtdGVzdCIsIk9SR19JRCI6IjY0IiwiaXNzIjoic3VtaXRAYmljeWNsZS5pbyIsImlhdCI6MTY0NDk0MTQxMywiVEVOQU5UIjoiZW10LWU5ZTRlZjZjLTYzYzQtNDkzMC1iMzMxLTJkZjNhZjFlNzg4ZSIsImp0aSI6IjBkZjU4ZmFkLTk0NzMtNDQ4OS1iNzMifQ.t8F2oEwEFej1xU2LknY2pLsbgUW3x5YED8trN9QYzDU";
        String connectorId = "8659da17-eb4b-417d-a762-9d59a85a9eef";
        String userId = "";
        String eventSourceType= "EVENT";
        String tenantId = "";

        BicycleConfig bicycleConfig = new BicycleConfig(serverURL, metricStoreURL, token, connectorId,uniqueIdentifier,
                tenantId, Mockito.mock(SystemAuthenticator.class),true);
        AuthInfo authInfo = bicycleConfig.getAuthInfo();
        EventSourceInfo eventSourceInfo = new EventSourceInfo(bicycleConfig.getConnectorId(), eventSourceType);

        JsonNode config = getGSCConfig();
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

    private JsonNode getPublicUrlConfig() {

        ObjectMapper mapper = new ObjectMapper();
        JsonNode config = mapper.createObjectNode();
        //((ObjectNode)config).put("url", "");
        //((ObjectNode)config).put("url", "file:///home/ravi/Desktop/test1.csv");
        ((ObjectNode) config).put("url", "https://storage.googleapis.com/kdev-blob-store/evt-fbb967ad-25f2-4eee-b2e5-f52b047be2ef/test-namespace/test-path/1ec89429-8516-43c5-8d2b-6cd22b63f248?GoogleAccessId=alert-store-bucket-access@pivotal-canto-171605.iam.gserviceaccount.com&Expires=1703045130&Signature=Lzkc2qVoaeBlE1S%2FHrhCr3Cc5HTVB0T5XJLDew9kpMniAPyI6DtfGf8RSs1baUWj6MDo%2B96JG7jXK4u07fpRTgT0MZeCmgmTHJwZOqLINtAEU2lzAluxQqNa6iovr5YVe9t0GuCq8jNX31Itws1m1H7aPCK%2B75M7RcrOZ8JrnWq8fGSR3FEu1wm6tC30fkrmT0fdEUXKNR2iK8KkUOy789ODs836vcJoW%2BSNcJKEUZC5%2FSznIWpJkZY6CiZPmLTodOZNJeiFtJ6gmvBr4coAPsk2vkXLRqAnno3sNR%2FEcxv2F5rBdh6hE8fR%2FkzVmIp1gasPE14KaTc8U1rG3wPHdA%3D%3D");
        ((ObjectNode) config).put("timeHeader", "Start Time");
        ((ObjectNode) config).put("timeFormat", "yyyy-MMM-dd HH:mm:ss z");
        ((ObjectNode) config).put("timeZone", "UTC");
        ((ObjectNode) config).put("datasetName", "test-csv");
        ((ObjectNode) config).put("format", "csv");
        ((ObjectNode) config).put("backfill", true);
        ((ObjectNode) config).put("replay", false);
        ((ObjectNode) config).put("mode", "prod");
        ((ObjectNode) config).put("backfillDateTime", "test");
        ((ObjectNode) config).put("backfillStartDateTime", "2023-06-01T05:30:00.000000Z");
        ((ObjectNode) config).put("backfillEndDateTime", "2023-07-01T05:29:59.000000Z");
        ((ObjectNode) config).put("publishEventsEnabled", "false");
        ((ObjectNode)config).put("storage", "publicUrl");

        JsonNode config1 = mapper.createObjectNode();
        ((ObjectNode)config1).put("storage", "publicUrl");
        ((ObjectNode)config1).put("service_account_json", "");
        ((ObjectNode)config).put("provider", config1);

        return config;

    }

    private JsonNode getGSCConfig() {

        ObjectMapper mapper = new ObjectMapper();
        JsonNode config = mapper.createObjectNode();
        //((ObjectNode)config).put("url", "");
        //((ObjectNode)config).put("url", "file:///home/ravi/Desktop/test1.csv");
        ((ObjectNode) config).put("url", "gs://inception-general-purpose-bucket/sumup/TransactionStatus-Allmonths.csv");
        ((ObjectNode) config).put("timeHeader", "CREATED_AT");
        ((ObjectNode) config).put("timeFormat", "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]['Z']");
        ((ObjectNode) config).put("timeZone", "UTC");
        ((ObjectNode) config).put("datasetName", "test-csv");
        ((ObjectNode) config).put("format", "csv");
        ((ObjectNode) config).put("backfill", true);
        ((ObjectNode) config).put("replay", false);
        ((ObjectNode) config).put("mode", "prod");
        ((ObjectNode) config).put("backfillDateTime", "test");
        ((ObjectNode) config).put("backfillStartDateTime", "2023-06-01T05:30:00.000000Z");
        ((ObjectNode) config).put("backfillEndDateTime", "2023-07-01T05:29:59.000000Z");
        ((ObjectNode) config).put("publishEventsEnabled", "false");
        ((ObjectNode)config).put("storage", "publicUrl");

        JsonNode config1 = mapper.createObjectNode();
        ((ObjectNode)config1).put("storage", "GCS");
        ((ObjectNode)config1).put("service_account_json", "{   \"type\": \"service_account\",   \"project_id\": \"inception-one\",   \"private_key_id\": \"f031677fd0945ea46b225702c91d072d7009e87b\",   \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCkAojeiP9gII3e\\nc3poTSKfcTKsjXlIv+SATQLgY/sAwpOs3Fdim/TlEnDKrEN1jkDS3Ss6C8ITPlod\\n70hGffPsIsamE/sCGtYf4oragiU3hj7OGqtBBD1VBrHt+6rWaQsI+ffRJJDO4gYZ\\n3U7Y0PE6uDdfWeBR2jvEPMIs+RVyHFU+tBlIc9+wkozvwEnk3M94hgNqeovaS87M\\np0LJ8nfmQvtiDpx/DmIA+5AUnJhp/mWEBVaq7Ej3TGzc3SIM5S32olB7nJD20h9s\\nAatNHDVj4QhcPI7FAcREqX41k9wIi+2tR74HPKWLbgJhzz42Z7BGyAiBlVsdxn72\\nVkJ8XPcFAgMBAAECggEAAI2C/3KmcMnMByiGWB2a/CeiFbQ1i/ZK4fsAd7rq5n+B\\nY95rboZm52Ez31h0su6qDyGzSsGiEh4fy61xN05VmRu3iLrmjifrf+FUhVds6EcR\\nwkHwb2qBIN6qyxWdlhW7r8P9jXLtK2IQaVYh9Q77xSiCSlwBNkBSkkquZ9xaa/JI\\nKd1vmvdaLGm8u2RvxaA4hEvTGSH98x7nrU1Csjx5xBgP/7QB/0fjQUVckvOZHoFo\\nk8cj9DlMYfao8uHCGhqVdg50DdV7XFMWoZ6xhi9x8f6K0Ge4S//E05E/Ps/3sAbG\\nGd7EfrPBGvNjaf8ACuu6OPXimuz6p7LG8Vrnaws2+QKBgQDU5bVSAod7TsF00PVg\\nZLPTe2R9d8NnDgT8QQmtWTMYU33jhMKaGV3tw5Cb2GsU3UuUkmGvyjQvEAM1PDrG\\n959SJipDL1IPwoOmX7D5uepfGW9PzPMGc4fDfrz4TjkXuLkJ2OlswMX7oiP6Snkj\\nOZbjrTXJ6b6KPxYjxqZQenTdvQKBgQDFNwhmj2HxwiQOQCeSHFAeV9HPAy3KIt38\\nbHLWAP9uFcYeRfcKdxoQHxjLXGsrZpQc7hFuqsxP903ZdzLFck080hHG2PwgwqD3\\n+htEUyUK22HZY0k+5DhvEJeVFfeLImEIHg/66iRjttQvNWJwvMJo0Pi17ClgsTLx\\n9k/G3r8e6QKBgQC9ToC7qroT4ETPfdsi2oi4fVku4+ah5Wpzb9WOCeoQMHWZcPyl\\nj/bgq+wTWA5noBtLwhoQ+SkLzB4+IQ9WyuslXgBoe1Rp5RmxQBebB0ErTO+YsvJK\\ng0JuiGy6ErxbposLAZEWfhfOGDALqFstAlF0pBlXMHyYa15hc4uBtlHitQKBgG2d\\n4Cp5adxhp37QQ+5flFy5PWIOB0aCSNbERLQUi+VZbuxmwSBtAOyTDEoEjYDrHEpU\\nPRZBEx0jfX7xVSQQG0RCEyVud/2RkL0kpEE+4aj+NY4KNK0jVwbMtyRjuFr6eep0\\nIze5Kw7NkXTH/HZjRL/T31nm8TzQYVvCJ6eHKb35AoGADvFKZtwUEDtzWG3cyy8S\\n5k3bezHVqZ5W9h3Xws2aAUHlcwT8a4BDTXIjzlA7cBAPclCbGhepIaecIx7aQNfw\\nsg9e3VJ7OcrZ0/Oog6BSIZGnlb9VixhwTmA3RrcQWAHa/yTkr3VyjA82+H81yUAh\\nvq7w981MH40EKvT4WWi77D0=\\n-----END PRIVATE KEY-----\\n\",   \"client_email\": \"inception-storage-bucket-sa@inception-one.iam.gserviceaccount.com\",   \"client_id\": \"110629284861373364445\",   \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",   \"token_uri\": \"https://oauth2.googleapis.com/token\",   \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",   \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/inception-storage-bucket-sa%40inception-one.iam.gserviceaccount.com\",   \"universe_domain\": \"googleapis.com\" }");
        ((ObjectNode)config).put("provider", config1);

        return config;

    }

}
