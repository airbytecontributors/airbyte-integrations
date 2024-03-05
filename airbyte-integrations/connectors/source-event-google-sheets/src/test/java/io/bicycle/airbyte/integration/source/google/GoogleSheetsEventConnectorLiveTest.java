package io.bicycle.airbyte.integration.source.google;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.integrations.source.event.google.sheets.GoogleSheetsEventConnector;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.integration.common.authInfo.BicycleAuthInfo;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.mockito.Mockito;

/**
 * @author sumitmaheshwari
 * Created on 03/03/2024
 */
public class GoogleSheetsEventConnectorLiveTest {


    public static void main(String[] args) throws Exception {
        testPreview();
    }
    public static void testPreview() throws Exception {

        System.setProperty("dev.mode", "true");
        ObjectMapper objectMapper = new ObjectMapper();
        String configString = readFileAsString("config.json");
        JsonNode config = objectMapper.readValue(configString, JsonNode.class);
        String catalogString = readFileAsString("catalog.json");
        AuthInfo authInfo = new BicycleAuthInfo("", "");
        SystemAuthenticator systemAuthenticator = Mockito.mock(SystemAuthenticator.class);
        Mockito.when(systemAuthenticator.authenticateAs(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString())).thenReturn(authInfo);


        GoogleSheetsEventConnector googleSheetsEventConnector =
                new GoogleSheetsEventConnector(systemAuthenticator,
                        Mockito.mock(EventConnectorJobStatusNotifier.class), null);


        ConfiguredAirbyteCatalog catalog = objectMapper.readValue(catalogString, ConfiguredAirbyteCatalog.class);

        System.out.println("Check result:: " + googleSheetsEventConnector.check(config));

        System.out.println("Discover result:: " + googleSheetsEventConnector.discover(config));

        AutoCloseableIterator<AirbyteMessage> iterator =
                googleSheetsEventConnector.preview(config, catalog, null);

        googleSheetsEventConnector.read(config, catalog, null);



        System.out.println(iterator.next());

    }

    public static String readFileAsString(String fileName) {

        ClassLoader classLoader = GoogleSheetsEventConnectorLiveTest.class.getClassLoader();
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
