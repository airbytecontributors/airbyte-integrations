package io.airbyte.integrations.source.event.bigquery.data.formatter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.SyncMode;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sumitmaheshwari
 * Created on 11/10/2023
 */
public class GoogleAnalyticsV4DataFormatter implements DataFormatter {
    private static final Logger logger = LoggerFactory.getLogger(GoogleAnalyticsV4DataFormatter.class.getName());
    @Override
    public JsonNode formatEvent(JsonNode jsonNode) {

        try {
            ArrayNode eventParams = (ArrayNode) jsonNode.get("event_params");
            for (int i = 0; i < eventParams.size(); i++) {
                JsonNode node = eventParams.get(i);
                updateNodeValue(node);
            }
        } catch (Exception e) {
            logger.error("Unable to format event_params for google analytics v4", e);
        }

        return jsonNode;
    }

    private static JsonNode updateNodeValue(JsonNode jsonNode) {

        JsonNode valueNode = jsonNode.get("value");
        String dataType = null;
        JsonNode actualNode = null;

        for (JsonNode node : valueNode) {
            if (!node.isNull()) {
                if (node.isInt()) {
                    dataType = "int";
                    actualNode = node;
                    break;
                } else if (node.isFloat()) {
                    dataType = "float";
                    actualNode = node;
                    break;
                } else if (node.isFloat()) {
                    dataType = "float";
                    actualNode = node;
                    break;
                } else if (node.isLong()) {
                    dataType = "long";
                    actualNode = node;
                    break;
                } else if (node.isTextual()) {
                    dataType = "string";
                    actualNode = node;
                    break;
                }
            }
        }

        if (dataType != null && actualNode != null) {
            ((ObjectNode) jsonNode).remove("value");
            ((ObjectNode) jsonNode).put("dataType", dataType);
            ((ObjectNode) jsonNode).set("value", actualNode);
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
        ConfiguredAirbyteStream stream = catalog.getStreams().get(0);
        stream.setSyncMode(SyncMode.INCREMENTAL);
        if (stream.getCursorField().size() == 0) {
            stream.getCursorField().add(getCursorFieldName());
        }
        return catalog;
    }
}
