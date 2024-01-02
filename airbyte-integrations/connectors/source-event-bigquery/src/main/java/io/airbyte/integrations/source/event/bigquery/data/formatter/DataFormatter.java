package io.airbyte.integrations.source.event.bigquery.data.formatter;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.source.event.bigquery.BigQueryEventSourceConfig;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import java.util.List;

/**
 * @author sumitmaheshwari
 * Created on 11/10/2023
 * This would be used to format the data based on the data format chosen.
 */
public interface DataFormatter {

  /***
   * Formats the events which in more simple format which is easy to use for mapping.
   * @param jsonNode
   * @return
   */
  JsonNode formatEvent(JsonNode jsonNode);

  /***
   * Returns the cursor field name for the specific data formatter.
   * @return
   */
  String getCursorFieldName();

  /***
   * Returns the cursor field value useful in updating the state when records are processed.
   * @param rawData
   * @return
   */
  String getCursorFieldValue(List<JsonNode> rawData);

  /***
   * Update the sync mode and add cursor field for streams that are added incrementally.
   * @param catalog
   * @return
   */
  ConfiguredAirbyteCatalog updateSyncMode(ConfiguredAirbyteCatalog catalog);

  /***
   * This is to filter the interested streams specific for that data formatter type and add it in catalog object.
   * @param connectorId
   * @param catalog
   * @param availableStreams
   * @return
   */
  ConfiguredAirbyteCatalog updateConfiguredAirbyteCatalogWithInterestedStreams(String connectorId,
                                                                               ConfiguredAirbyteCatalog catalog,
                                                                               List<AirbyteStream> availableStreams,
                                                                               BigQueryEventSourceConfig bigQueryEventSourceConfig);

  void publishLagMetrics(EventSourceInfo eventSourceInfo, String stateAsString);

}
