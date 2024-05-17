package io.airbyte.integrations.source.event.bigquery;

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
public class BigQueryStreamGetter implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(BigQueryStreamGetter.class.getName());
    public static final long LAST_1_DAY_MILLISECONDS = 1 * 24 * 60 * 60 * 1000;
    public static final String SECONDS = "seconds";
    public static final String MILLIS = "millis";
    public static final String MICROS = "micros";
    private final JsonNode config;
    private final BigQueryEventSource bigQueryEventSource;
    private final String connectorId;
    private List<AirbyteStream> streamList = new ArrayList<>();

    public BigQueryStreamGetter(String connectorId, BigQueryEventSource bigQueryEventSource, JsonNode config,
                                List<AirbyteStream> streamList) {
        this.bigQueryEventSource = bigQueryEventSource;
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
            AirbyteCatalog catalog = bigQueryEventSource.discover(config);
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
