package io.airbyte.integrations.source.event.bigquery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.source.relationaldb.models.DbState;
import io.airbyte.integrations.source.relationaldb.models.DbStreamState;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.AirbyteStreamState;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author sumitmaheshwari
 * Created on 11/10/2023
 */
public class TestStateCreation {

  private static ObjectMapper objectMapper = new ObjectMapper();

  public static void main(String[] args) throws JsonProcessingException {

    ConfiguredAirbyteCatalog  catalog = new ConfiguredAirbyteCatalog();
    ConfiguredAirbyteStream configuredAirbyteStream = new ConfiguredAirbyteStream();
    AirbyteStream airbyteStream = new AirbyteStream();
    airbyteStream.setName("stream1");
    airbyteStream.setNamespace("stream1namespace");
    configuredAirbyteStream.setStream(airbyteStream);
    catalog.getStreams().add(configuredAirbyteStream);

    AirbyteStateMessage message = createStateMessage(catalog, "field1", "field1Value");
    System.out.println(objectMapper.writeValueAsString(message.getData()));

  }

  private static AirbyteStateMessage createStateMessage(ConfiguredAirbyteCatalog catalog,
                                                 String cursorField, String cursorFieldValue) {
    DbState dbState = new DbState();
    dbState.setCdc(true);
    DbStreamState dbStreamState = new DbStreamState();

    List<String> cursorFields = new ArrayList<>();
    cursorFields.add(cursorField);

    AirbyteStreamState airbyteStreamState = new AirbyteStreamState();
    ConfiguredAirbyteStream stream = catalog.getStreams().get(0);
    airbyteStreamState.setName(stream.getStream().getName());

    dbStreamState.setStreamName(stream.getStream().getName());
    dbStreamState.setStreamNamespace(stream.getStream().getNamespace());
    dbStreamState.setCursorField(cursorFields);
    dbStreamState.setCursor(cursorFieldValue);

    dbState.getStreams().add(dbStreamState);

    return new AirbyteStateMessage().withData(Jsons.jsonNode(dbState));
  }

}
