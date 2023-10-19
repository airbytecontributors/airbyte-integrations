package io.airbyte.integrations.source.event.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.inception.server.auth.api.SystemAuthenticator;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.event.processor.api.BicycleEventProcessor;
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

        BigQueryEventSource bigQueryEventSource = new BigQueryEventSource(Mockito.mock(SystemAuthenticator.class),
                Mockito.mock(EventConnectorJobStatusNotifier.class), null);


        ObjectMapper objectMapper = new ObjectMapper();
        String configString = readFileAsString("config.json");
        JsonNode config = objectMapper.readValue(configString, JsonNode.class);
        String catalogString = readFileAsString("catalog.json");

        String stateString = readFileAsString("state.json");
        JsonNode state = objectMapper.readValue(stateString, JsonNode.class);

        ConfiguredAirbyteCatalog catalog = objectMapper.readValue(catalogString, ConfiguredAirbyteCatalog.class);
        bigQueryEventSource.read(config, catalog, null);

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
