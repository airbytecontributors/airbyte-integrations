package io.airbyte.integrations.source.event.bigquery.data.formatter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.SyncMode;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sumitmaheshwari
 * Created on 11/10/2023
 */
public class GoogleAnalyticsV4DataFormatter implements DataFormatter {
    private static final Logger logger = LoggerFactory.getLogger(GoogleAnalyticsV4DataFormatter.class.getName());
    private static final String EVENT_PARAMS_ATTRIBUTE = "event_params";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public JsonNode formatEvent(JsonNode jsonNode) {

        try {
            ArrayNode eventParams = (ArrayNode) jsonNode.get(EVENT_PARAMS_ATTRIBUTE);
            JsonNode outputNode = objectMapper.createObjectNode();

            for (int i = 0; i < eventParams.size(); i++) {
                JsonNode node = eventParams.get(i);
                String key = node.get("key").asText();
                JsonNode valueNode = node.get("value");

                // get non-null value
                String stringValue = valueNode.has("string_value") ? valueNode.get("string_value").asText(null) : null;
                Integer intValue = valueNode.has("int_value") ? valueNode.get("int_value").asInt(Integer.MIN_VALUE) : null;
                Float floatValue = valueNode.has("float_value") ? valueNode.get("float_value").floatValue() : null;
                Double doubleValue = valueNode.has("double_value") ? valueNode.get("double_value").doubleValue() : null;
                Long longValue = valueNode.has("long_value") ? valueNode.get("long_value").longValue() : null;

                if (stringValue != null) {
                    ((ObjectNode) outputNode).put(key, stringValue);
                } else if(intValue != null) {
                    ((ObjectNode) outputNode).put(key, intValue);
                } else if(floatValue != null) {
                    ((ObjectNode) outputNode).put(key, floatValue);
                } else if(doubleValue != null) {
                    ((ObjectNode) outputNode).put(key, doubleValue);
                } else if(longValue != null) {
                    ((ObjectNode) outputNode).put(key, longValue);
                }

            }

            ((ObjectNode)jsonNode).put(EVENT_PARAMS_ATTRIBUTE, outputNode);

            return jsonNode;
        } catch (Exception e) {
            logger.error("Unable to format event_params for google analytics v4", e);
        }

        return jsonNode;
    }

    @Override
    public String getCursorFieldName() {
        return "event_timestamp";
    }

    @Override
    public String getCursorFieldValue(List<JsonNode> rawData) {
        long maxTimestamp = 0;

        try {
            for (JsonNode jsonNode : rawData) {
                long timestamp = jsonNode.get(getCursorFieldName()).asLong();
                if (timestamp > maxTimestamp) {
                    maxTimestamp = timestamp;
                }
            }
        } catch (Exception e) {
          logger.error("Unable to get cursor field value returning 0", e);
        }

        return String.valueOf(maxTimestamp);
    }

    @Override
    public ConfiguredAirbyteCatalog updateSyncMode(ConfiguredAirbyteCatalog catalog) {
       // ConfiguredAirbyteStream stream = catalog.getStreams().get(0);
        List<ConfiguredAirbyteStream> streams = catalog.getStreams();

        for (ConfiguredAirbyteStream stream: catalog.getStreams()) {
            stream.setSyncMode(SyncMode.INCREMENTAL);
            if (stream.getCursorField().size() == 0) {
                stream.getCursorField().add(getCursorFieldName());
            }
        }
      /*  stream.setSyncMode(SyncMode.INCREMENTAL);
        if (stream.getCursorField().size() == 0) {
            stream.getCursorField().add(getCursorFieldName());
        }*/
        return catalog;
    }

    @Override
    public ConfiguredAirbyteCatalog updateConfiguredAirbyteCatalogWithInterestedStreams(
            String connectorId, ConfiguredAirbyteCatalog catalog, List<AirbyteStream> availableStreams) {

        try {
            List<ConfiguredAirbyteStream> interestedStreams = new ArrayList<>();
            for (AirbyteStream airbyteStream : availableStreams) {
                String name = airbyteStream.getName();
                if (name.contains("intraday")) {
                    ConfiguredAirbyteStream configuredAirbyteStream = new ConfiguredAirbyteStream();
                    configuredAirbyteStream.setStream(airbyteStream);
                    interestedStreams.add(configuredAirbyteStream);
                }
            }
            catalog.getStreams().clear();
            catalog.getStreams().addAll(interestedStreams);
        } catch (Exception e) {
            logger.error("Unable to update catalog with interested streams for connector Id {} {} ", connectorId,
                    e);
        }

        catalog = updateSyncMode(catalog);

        return catalog;
    }
}
