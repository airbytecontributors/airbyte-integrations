package io.airbyte.integrations.source.event.bigquery.data.formatter;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.util.List;

/**
 * @author sumitmaheshwari
 * Created on 11/10/2023
 * This would be used to format the data based on the data format chosen.
 */
public interface DataFormatter {

  JsonNode formatEvent(JsonNode jsonNode);
  String getCursorFieldName();
  String getCursorFieldValue(List<JsonNode> rawData);

  ConfiguredAirbyteCatalog updateSyncMode(ConfiguredAirbyteCatalog catalog);

  ConfiguredAirbyteCatalog updateConfiguredAirbyteCatalogWithInterestedStreams(String connectorId,
                                                                               ConfiguredAirbyteCatalog catalog,
                                                                               List<AirbyteStream> availableStreams);

}
