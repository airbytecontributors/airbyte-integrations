package io.airbyte.integrations.bicycle.base.integration.reader.json;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.integrations.bicycle.base.integration.BaseCSVEventConnector;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.reader.EventSourceReader;
import io.airbyte.integrations.bicycle.base.integration.reader.csv.CSVEventSourceReader;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonLEventSourceReader extends EventSourceReader<RawEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVEventSourceReader.class);

    protected String connectorId;
    protected File jsonFile;
    protected RawEvent nextEvent;
    protected String name;
    protected long counter = 0;
    protected long nullRows = 0;
    protected BufferedReader bufferedReader;

    public JsonLEventSourceReader(String name, File jsonFile, String connectorId,
                                  BaseEventConnector connector,
                                  BaseCSVEventConnector.APITYPE apiType) {
        super(jsonFile.getName(), connectorId, connector, apiType);
        this.name = name;
        this.jsonFile = jsonFile;
        initialize();
    }

    private void initialize() {
        try {
            bufferedReader = new BufferedReader(new FileReader(jsonFile, Charset.defaultCharset()));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean hasNext() {
        reset();
        do {
            try {
              //  offset = accessFile.getFilePointer();
                row = bufferedReader.readLine();
                rowCounter++;
                if (!StringUtils.isEmpty(row)) {
                    return true;
                } else if (StringUtils.isEmpty(row)) {
                    nullRows++;
                    continue;
                } else {
                    LOGGER.info("Exiting as coming in else block for stream Id {} and counter is at {}",
                            connectorId, counter);
                    break;
                }
            } catch (Exception e) {
                LOGGER.error("Error while calculating timestamp to offset map for a row [{}] for stream Id [{}]",
                        row, connectorId, e);
            }
        } while (nullRows < 200);
        return false;
    }

    private void reset() {
        validEvent = true;
        row = null;
        offset = -1;
        nextEvent = null;
    }

    public RawEvent next() {
        String errorMessage = null;
        try {
            counter++;
            ObjectNode node = getJsonRecord(rowCounter, row);
            if (node != null) {
                nextEvent = convertRecordsToRawEvents(node, rowCounter, name);
                return nextEvent;
            }
        } catch (Exception e) {
            validEvent = false;
            errorMessage = "[" + e.getMessage() + "] [" + rowCounter + "] [" + row + "]";
            LOGGER.error("Failed Parsing [" + row + "] [" + rowCounter + "]", e);
        } finally {
            if (!isValidEvent()) {
                status = BaseEventConnector.ReaderStatus.FAILED;
            }
        }
        ObjectNode node = mapper.createObjectNode();
        node.put("bicycle.raw.event.record", row);
        nextEvent = getJsonRawEvent(rowCounter, name, node, errorMessage);
        return nextEvent;
    }

    private ObjectNode getJsonRecord(long rowCounter, String row) {
        String streamId = connectorId;
        try {
            ObjectNode jsonNode = (ObjectNode) mapper.readTree(row);
            validEvent = true;
            return jsonNode;
        } catch (Throwable e) {
            LOGGER.error("Failed to parse the row [{}] [{}] for stream Id [{}]. Row will be ignored",
                    rowCounter, row, streamId, e);
            validEvent = false;
        }
        return null;
    }

    public RawEvent convertRecordsToRawEvents(ObjectNode node, long rowCounter, String fileName)
            throws Exception {
        String errorMessage = null;
        if (!validEvent) {
            node.put("bicycle.raw.event.record", row);
            errorMessage = "Error";
        }
        JsonRawEvent jsonRawEvent = getJsonRawEvent(rowCounter, fileName, node, errorMessage);
        return jsonRawEvent;
    }

    @Override
    public void close() throws Exception {
       /* if (accessFile != null) {
            accessFile.close();
        }*/
    }
}
