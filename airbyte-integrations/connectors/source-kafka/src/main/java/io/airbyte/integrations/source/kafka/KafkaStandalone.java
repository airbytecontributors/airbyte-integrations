package io.airbyte.integrations.source.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;

/**
 * @author sumitmaheshwari
 * Created on 05/09/2023
 */
public class KafkaStandalone {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {

        ConfiguredAirbyteCatalog configuredAirbyteCatalog = new ConfiguredAirbyteCatalog();
        ConnectorConfigManager connectorConfigManager
        KafkaSource kafkaSource = new KafkaSource(null, null, null);

        configuredAirbyteCatalog.getAdditionalProperties().put("bicycleServerURL", "https://api.dev.bicycle.io");
        configuredAirbyteCatalog.getAdditionalProperties()
                .put("bicycleConnectorId", "fb62555c-5cc5-4949-8a67-dc502dda2772");
        configuredAirbyteCatalog.getAdditionalProperties().put("bicycleEventSourceType", "source-kafka");
        configuredAirbyteCatalog.getAdditionalProperties().put("bicycleMetricStoreURL",
                "http://anom-metric-store.bha.svc.cluster.local:4242/api/anoms/api/put?details");
        configuredAirbyteCatalog.getAdditionalProperties().put("isOnPrem", "false");
        configuredAirbyteCatalog.getAdditionalProperties().put("bicycleToken", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJST0xFIjoiQVBJIiwic3ViIjoic3VtaXQtdGVzdCIsIk9SR19JRCI6IjY0IiwiaXNzIjoic3VtaXRAYmljeWNsZS5pbyIsImlhdCI6MTY0NDk0MTQxMywiVEVOQU5UIjoiZW10LWU5ZTRlZjZjLTYzYzQtNDkzMC1iMzMxLTJkZjNhZjFlNzg4ZSIsImp0aSI6IjBkZjU4ZmFkLTk0NzMtNDQ4OS1iNzMifQ.t8F2oEwEFej1xU2LknY2pLsbgUW3x5YED8trN9QYzDU");
        configuredAirbyteCatalog.getAdditionalProperties()
                .put("bicycleTenantId", "emt-e9e4ef6c-63c4-4930-b331-2df3af1e788e");

        AirbyteStream airbyteStream = new AirbyteStream();
        airbyteStream.setName("test-proto");
        ConfiguredAirbyteStream configuredAirbyteStream = new ConfiguredAirbyteStream();
        configuredAirbyteStream.setStream(airbyteStream);
        configuredAirbyteCatalog.getStreams().add(configuredAirbyteStream);

        kafkaSource.read(getConfig(), configuredAirbyteCatalog, null);

    }

    private static JsonNode getConfig() {
        try {
            String jsonFilePath = "config.json";
            // Use the ClassLoader to load the resource as an InputStream
            InputStream inputStream = KafkaStandalone.class.getClassLoader().getResourceAsStream(jsonFilePath);

            if (inputStream != null) {
                // Read the JSON content from the InputStream
                String jsonString = IOUtils.toString(inputStream, "UTF-8");
                JsonNode jsonNode = objectMapper.readTree(jsonString);
                // Now you can work with the JSON data as needed
                System.out.println(jsonNode); // Print formatted JSON
                // Close the InputStream when you're done
                inputStream.close();
                return jsonNode;
            } else {
                System.err.println("Resource not found: " + jsonFilePath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
