package io.airbyte.integrations.bicycle.base.integration.reader.csv;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.integrations.bicycle.base.integration.BaseCSVEventConnector;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.reader.EventSourceReader;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.server.event.mapping.UserServiceFieldDef;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.RandomAccessFile;
import java.io.StringReader;
import java.net.URL;
import java.util.*;


public class CSVEventSourceReader extends EventSourceReader<RawEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVEventSourceReader.class);

    protected String connectorId;
    protected URL url;
    RandomAccessFile accessFile = null;
    protected Map<String, Integer> headerNameToIndexMap;
    protected RawEvent nextEvent;
    protected CSVRecord csvRecord;
    protected String name;
    protected long counter = 0;
    protected long nullRows = 0;

    public CSVEventSourceReader(String name, URL url, String connectorId,
                                BaseEventConnector connector, BaseCSVEventConnector.APITYPE apiType) {
        super(name, connectorId, connector, apiType);
        this.name = name;
        this.url = url;
        initialize();
    }

    protected void initialize() {
        try {
            String csvFile = url.getFile();
            accessFile = new RandomAccessFile(csvFile, "r");
            String headersLine = accessFile.readLine();
            rowCounter++;
            headerNameToIndexMap = getHeaderNameToIndexMap(headersLine);
            if (headerNameToIndexMap.isEmpty()) {
                throw new RuntimeException("Unable to read headers from csv file " + name + " for " + connectorId);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read csv file [" + name + "]", e);
        }
    }

    private void reset() {
        validEvent = true;
        row = null;
        offset = -1;
        nextEvent = null;
        csvRecord = null;
    }

    public void seek(long offset, long rowCounter) throws Exception {
        this.accessFile.seek(offset);
        this.row = accessFile.readLine();
        this.offset = offset;
        this.rowCounter = rowCounter;
    }

    public boolean hasNext() {
        reset();
        do {
            try {
                offset = accessFile.getFilePointer();
                row = accessFile.readLine();
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

    public RawEvent next() {
        String errorMessage = null;
        try {
            counter++;
            csvRecord = getCsvRecord(rowCounter, row, headerNameToIndexMap);
            if (csvRecord != null) {
                nextEvent = convertRecordsToRawEvents(headerNameToIndexMap, csvRecord, rowCounter, name);
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

    private UserServiceFieldDef findStartTimeMicros(List<UserServiceFieldDef> fieldsList) {
        for (UserServiceFieldDef userServiceFieldDef : fieldsList) {
            if (userServiceFieldDef.getPredefinedFieldType().equals("startTimeMicros")) {
                return userServiceFieldDef;
            }
        }
        return null;
    }

    public String getJson() {
        Map<String, String> jsonMap = new HashMap<>();
        try {
            for (String headerName : headerNameToIndexMap.keySet()) {
                headerName = headerName.replaceAll("\\.", "_");
                jsonMap.put(headerName, csvRecord.get(headerName));
            }
        } catch (Exception e) {
            LOGGER.error("Unable to convert a row to json", e);
            return null;
        }


        try {
            return mapper.writeValueAsString(jsonMap);
        } catch (Exception e) {
            LOGGER.error("Unable to convert a csv row {} to json because of {}", csvRecord, e);
            return null; // Return an empty JSON object in case of an error
        }
    }

    public void close() throws Exception {
        if (accessFile != null) {
            accessFile.close();
        }
    }

    private CSVRecord getCsvRecord(long rowCounter, String row, Map<String, Integer> headerNameToIndexMap) {
        String streamId = connectorId;
        try {
            CSVParser csvParser = CSVParser.parse(new StringReader(row), CSVFormat.DEFAULT);
            CSVRecord record = csvParser.iterator().next();
            int columns = record.size();
            if (columns != headerNameToIndexMap.size()) {
                LOGGER.warn("Ignoring the row {} for stream Id {}", row, streamId);
            }
            return record;
        } catch (Throwable e) {
            LOGGER.error("Failed to parse the row [{}] [{}] for stream Id [{}]. Row will be ignored",
                    rowCounter, row, streamId, e);
        }
        return null;
    }

    private static Map<String, Integer> getHeaderNameToIndexMap(String row) {
        Map<String, Integer> fieldNameToIndexMap = new HashMap<>();
        try {
            CSVParser csvRecords = new CSVParser(new StringReader(row), CSVFormat.DEFAULT.withSkipHeaderRecord());
            CSVRecord next = csvRecords.iterator().next();
            Iterator<String> iterator = next.iterator();
            int index = 0;
            while (iterator.hasNext()) {
                String headerName = iterator.next();
                headerName = headerName.replaceAll("\\.", "_");
                fieldNameToIndexMap.put(headerName, index);
                index++;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to parse the row using csv reader " + row, e);
        }

        return fieldNameToIndexMap;
    }

    public RawEvent convertRecordsToRawEvents(Map<String, Integer> headerNameToIndexMap, CSVRecord record,
                                              long rowCounter, String fileName)
            throws Exception {
        ObjectNode node = mapper.createObjectNode();
        if (headerNameToIndexMap.size() != record.size()) {
            validEvent = false;
        } else {
            validEvent = true;
        }
        String errorMessage = null;
        if (validEvent) {
            for (String key : headerNameToIndexMap.keySet()) {
                int index = headerNameToIndexMap.get(key);
                String value = record.get(index);
                node.put(key, value);
            }
        } else {
            node.put("bicycle.raw.event.record", row);
            errorMessage = "Headers and fields count does not match";
        }
        JsonRawEvent jsonRawEvent = getJsonRawEvent(rowCounter, fileName, node, errorMessage);
        return jsonRawEvent;
    }

}
