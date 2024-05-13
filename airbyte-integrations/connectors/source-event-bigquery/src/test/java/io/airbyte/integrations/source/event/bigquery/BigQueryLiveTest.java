package io.airbyte.integrations.source.event.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.entity.mapping.SourceFieldMapping;
import io.bicycle.entity.mapping.SourceValueMapping;
import io.bicycle.event.processor.api.BicycleEventProcessor;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.integration.connector.scrub.SimpleSplitEventConfig;
import io.bicycle.integration.connector.scrub.SplitEventConfig;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.mockito.Mockito;

/**
 * @author sumitmaheshwari
 * Created on 10/10/2023
 */
public class BigQueryLiveTest {

    public static void main(String[] args) throws Exception {

        ConnectorConfigManager connectorConfigManager = Mockito.mock(ConnectorConfigManager.class);

        SplitEventConfig.Builder splitEventConfigBuilder = SplitEventConfig.newBuilder();
        SimpleSplitEventConfig.Builder simpleSplitEventConfigBuilder = SimpleSplitEventConfig.newBuilder();
        String sourceJsonPath = "$.hits";
        String destinationJsonPath = "$.hit";

        SourceFieldMapping sourceFieldMapping = SourceFieldMapping.newBuilder().setValueMapping
                (SourceValueMapping.newBuilder().setJsonPath(sourceJsonPath).build()).build();
        simpleSplitEventConfigBuilder.setSplitFieldMapping(sourceFieldMapping);
        simpleSplitEventConfigBuilder.setDestinationJsonPath(destinationJsonPath);
        simpleSplitEventConfigBuilder.setAddAsArray(false);

        splitEventConfigBuilder.setSimpleSplitEventConfig(simpleSplitEventConfigBuilder.build());

        Mockito.when(connectorConfigManager.getSplitEventConfig(Mockito.any(AuthInfo.class), Mockito.any(String.class)))
                .thenReturn(splitEventConfigBuilder.build());

        BigQueryEventSource bigQueryEventSource = new BigQueryEventSource(Mockito.mock(SystemAuthenticator.class),
                Mockito.mock(EventConnectorJobStatusNotifier.class), connectorConfigManager);


        ObjectMapper objectMapper = new ObjectMapper();
        String configString = readFileAsString("config_avis_split_kdev.json");
        JsonNode config = objectMapper.readValue(configString, JsonNode.class);
        String catalogString = readFileAsString("catalog_avis_split_kdev.json");

        String stateString = readFileAsString("state.json");
        JsonNode state = objectMapper.readValue(stateString, JsonNode.class);

        ConfiguredAirbyteCatalog catalog = objectMapper.readValue(catalogString, ConfiguredAirbyteCatalog.class);
        bigQueryEventSource.read(config, catalog, null);

      //  AirbyteCatalog airbyteCatalog = bigQueryEventSource.discover(config);
      //  AutoCloseableIterator<AirbyteMessage> iterator = bigQueryEventSource.preview(config, catalog, null);
      //  iterator.next();
       /* AutoCloseableIterator<AirbyteMessage> iterator =
                bigQueryEventSource.preview(config, catalog, state);
        System.out.println(iterator.hasNext() ? iterator.next() : "null");
*/
    }



    public static String readFileAsString(String fileName) {

        ClassLoader classLoader = BicycleEventProcessor.class.getClassLoader();
        File file = new File(classLoader.getResource(fileName).getFile());

        try {

            String content = Files.readString(file.toPath(), StandardCharsets.US_ASCII);
            return content;
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
        return null;
    }

}
