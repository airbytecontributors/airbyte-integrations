package io.airbyte.integrations.source.event.snowflake;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.integrations.source.event.snowflake.SnowflakeEventSource;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.event.processor.api.BicycleEventProcessor;
import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.integration.connector.runtime.BackFillConfiguration;
import io.bicycle.integration.connector.runtime.RuntimeConfig;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;
import org.mockito.Mockito;

/**
 * @author sumitmaheshwari
 * Created on 10/10/2023
 */
public class SnowflakeEventLiveTest {

    public static void main(String[] args) throws Exception {

        //testWithoutBackFill();
        //testWithBackFill();
        testWithNewTable();

    }

    public static void testWithNewTable() throws Exception {

        RuntimeConfig runtimeConfig = RuntimeConfig.newBuilder()
                .setBackFillConfig(BackFillConfiguration.newBuilder().setEnableBackFill(true)
                        .setEndTimeInMillis(1711843210000L).build()).build();


        ConnectorConfigManager connectorConfigManager = Mockito.mock(ConnectorConfigManager.class);
        Mockito.when(connectorConfigManager.getRuntimeConfig(Mockito.any(AuthInfo.class),
                Mockito.any(String.class))).thenReturn(runtimeConfig);

        SystemAuthenticator systemAuthenticator = Mockito.mock(SystemAuthenticator.class);
        SnowflakeEventSource snowFlakeEventSource = new SnowflakeEventSource(Mockito.mock(SystemAuthenticator.class),
                Mockito.mock(EventConnectorJobStatusNotifier.class), connectorConfigManager);

        ObjectMapper objectMapper = new ObjectMapper();
        String configString = readFileAsString("snowflake-sumit-payfactory-config.json");
        JsonNode config = objectMapper.readValue(configString, JsonNode.class);


        String catalogString = readFileAsString("snowflake-sumit-payfactory-catalog.json");
        ConfiguredAirbyteCatalog configuredAirbyteCatalog = objectMapper.readValue(catalogString, ConfiguredAirbyteCatalog.class);
        BicycleConfig bicycleConfig = snowFlakeEventSource.getBicycleConfig(configuredAirbyteCatalog.getAdditionalProperties(),
                systemAuthenticator);

        snowFlakeEventSource.setBicycleEventProcessorAndPublisher(bicycleConfig);




        // System.out.println(snowFlakeEventSource.check(config));

        AirbyteCatalog catalog = snowFlakeEventSource.discover(config);
        //  System.out.println(catalog);

        System.out.println(objectMapper.writeValueAsString(catalog.getStreams().get(0)));



        snowFlakeEventSource.read(config, configuredAirbyteCatalog, null);

    }


    public static void testWithBackFill() throws Exception {


        RuntimeConfig runtimeConfig = RuntimeConfig.newBuilder()
                .setBackFillConfig(BackFillConfiguration.newBuilder().setEnableBackFill(true)
                        .setEndTimeInMillis(10000).build()).build();


        ConnectorConfigManager connectorConfigManager = Mockito.mock(ConnectorConfigManager.class);
        Mockito.when(connectorConfigManager.getRuntimeConfig(Mockito.any(AuthInfo.class),
                Mockito.any(String.class))).thenReturn(runtimeConfig);

        SnowflakeEventSource snowFlakeEventSource = new SnowflakeEventSource(Mockito.mock(SystemAuthenticator.class),
                Mockito.mock(EventConnectorJobStatusNotifier.class), connectorConfigManager);


        ObjectMapper objectMapper = new ObjectMapper();
        String configString = readFileAsString("snowflake-sumit-backfill-config.json");
        JsonNode config = objectMapper.readValue(configString, JsonNode.class);

        // System.out.println(snowFlakeEventSource.check(config));

        AirbyteCatalog catalog = snowFlakeEventSource.discover(config);
        //  System.out.println(catalog);

        System.out.println(objectMapper.writeValueAsString(catalog.getStreams().get(0)));

        String catalogString = readFileAsString("snowflake-sumit-backfill-catalog.json");
        ConfiguredAirbyteCatalog configuredAirbyteCatalog = objectMapper.readValue(catalogString, ConfiguredAirbyteCatalog.class);
     /*   AutoCloseableIterator<AirbyteMessage> iterator =
                snowFlakeEventSource.preview(config, configuredAirbyteCatalog, null);
        System.out.println(iterator.next());*/

        snowFlakeEventSource.read(config, configuredAirbyteCatalog, null);


    }

    public static void testWithoutBackFill() throws Exception {

        SnowflakeEventSource snowFlakeEventSource = new SnowflakeEventSource(Mockito.mock(SystemAuthenticator.class),
                Mockito.mock(EventConnectorJobStatusNotifier.class), null);


        ObjectMapper objectMapper = new ObjectMapper();
        String configString = readFileAsString("snowflake-sumit-config.json");
        JsonNode config = objectMapper.readValue(configString, JsonNode.class);

        // System.out.println(snowFlakeEventSource.check(config));

        AirbyteCatalog catalog = snowFlakeEventSource.discover(config);
        //  System.out.println(catalog);

        System.out.println(objectMapper.writeValueAsString(catalog.getStreams().get(0)));

        String catalogString = readFileAsString("snowflake-sumit-catalog.json");
        ConfiguredAirbyteCatalog configuredAirbyteCatalog = objectMapper.readValue(catalogString, ConfiguredAirbyteCatalog.class);
     /*   AutoCloseableIterator<AirbyteMessage> iterator =
                snowFlakeEventSource.preview(config, configuredAirbyteCatalog, null);
        System.out.println(iterator.next());*/

        snowFlakeEventSource.read(config, configuredAirbyteCatalog, null);


    }

    public static void testDiscover() {

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
