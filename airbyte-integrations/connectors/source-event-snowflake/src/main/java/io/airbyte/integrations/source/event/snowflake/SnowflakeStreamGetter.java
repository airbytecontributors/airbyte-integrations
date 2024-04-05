package io.airbyte.integrations.source.event.snowflake;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteStream;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sumitmaheshwari
 * Created on 18/10/2023
 */
public class SnowflakeStreamGetter implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SnowflakeStreamGetter.class.getName());
    public static final long LAST_7_DAYS_MILLISECONDS = 7 * 24 * 60 * 60 * 1000;
    private final JsonNode config;
    private final SnowflakeEventSource snowFlakeEventSource;
    private final String connectorId;
    private List<AirbyteStream> streamList = new ArrayList<>();

    public SnowflakeStreamGetter(String connectorId, SnowflakeEventSource snowFlakeEventSource, JsonNode config,
                                 List<AirbyteStream> streamList) {
        this.snowFlakeEventSource = snowFlakeEventSource;
        this.config = config;
        this.connectorId = connectorId;
        this.streamList = streamList;
    }

    public List<AirbyteStream> getStreamList() {
        return streamList;
    }

    public List<String> getStreamNames() {
        List<String> streams = new ArrayList<>();
        for (AirbyteStream stream : streamList) {
            streams.add(stream.getName());
        }
        return streams;
    }

    @Override
    public void run() {

        try {
            AirbyteCatalog catalog = snowFlakeEventSource.discover(config);
            streamList = catalog.getStreams();

            List<String> streams = new ArrayList<>();
            for (AirbyteStream stream : streamList) {
                streams.add(stream.getName());
            }
            logger.info("Fetched streams {}", streams);
        } catch (Exception e) {
            logger.warn("Unable to get streams for connector Id {} {}", connectorId, e);
        }
    }
}
