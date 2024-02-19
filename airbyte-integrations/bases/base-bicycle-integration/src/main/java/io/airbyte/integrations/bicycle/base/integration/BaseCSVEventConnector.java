package io.airbyte.integrations.bicycle.base.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inception.server.auth.api.SystemAuthenticator;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public abstract class BaseCSVEventConnector extends BaseEventConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseCSVEventConnector.class);

    public BaseCSVEventConnector(SystemAuthenticator systemAuthenticator,
                                 EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier,
                                 ConnectorConfigManager connectorConfigManager) {
        super(systemAuthenticator, eventConnectorJobStatusNotifier, connectorConfigManager);
    }

    protected int totalRecords(File file) {
        try (Reader reader = new FileReader(file);
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT)) {
            int recordCount = 0;
            for (CSVRecord record : csvParser) {
                recordCount++;
            }
            return recordCount;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse csv file["+file+"]");
        }
    }

    public static class CSVEventSourceReader extends EventSourceReader<RawEvent> {

        private String connectorId;
        private File csvFile;
        RandomAccessFile accessFile = null;
        Map<String, Integer> headerNameToIndexMap;
        private boolean validEvent = false;
        private RawEvent nextEvent;

        private BaseEventConnector connector;

        protected ObjectMapper mapper = new ObjectMapper();

        private String name;
        long offset = -1;
        String row = null;
        long counter = 0;
        long nullRows = 0;

        public CSVEventSourceReader(String name, File csvFile, String connectorId, BaseEventConnector connector) {
            this.name = name;
            this.connectorId = connectorId;
            this.csvFile = csvFile;
            this.connector = connector;
            initialize();
        }

        private void initialize() {
            try {
                accessFile = new RandomAccessFile(csvFile, "r");
                String headersLine = accessFile.readLine();
                headerNameToIndexMap = getHeaderNameToIndexMap(headersLine);
                if (headerNameToIndexMap.isEmpty()) {
                    throw new RuntimeException("Unable to read headers from csv file " + csvFile + " for " + connectorId);
                }
            } catch (Exception e) {
                throw new IllegalStateException("Failed to read csv file ["+csvFile+"]", e);
            }
        }

        private void reset() {
            validEvent = false;
            row = null;
            offset = -1;
            nextEvent = null;
        }

        public boolean hasNext() {
            reset();
            do {
                try {
                    row = accessFile.readLine();
                    offset = accessFile.getFilePointer();
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
            } while (nullRows < 5000);
            return false;
        }

        public boolean isValidEvent() {
            return validEvent;
        }

        public RawEvent next() {
            CSVRecord csvRecord = getCsvRecord(offset, row, headerNameToIndexMap);
            try {
                nextEvent = convertRecordsToRawEvents(headerNameToIndexMap, csvRecord, offset, name);
                if (headerNameToIndexMap.size() != csvRecord.size()) {
                    validEvent = false;
                } else {
                    validEvent = true;
                }
                counter++;
                return nextEvent;
            } catch (Exception e) {
                throw new IllegalStateException("Failed Parsing ["+row+"] ["+offset+"]");
            }
        }

        public void close() throws Exception {
            if (accessFile != null) {
                accessFile.close();
            }
        }

        private CSVRecord getCsvRecord(long offset, String row, Map<String, Integer> headerNameToIndexMap) {
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
                        offset, row, streamId, e);
            }
            return null;
        }

        private static  Map<String, Integer> getHeaderNameToIndexMap(String row) {
            Map<String, Integer> fieldNameToIndexMap = new HashMap<>();
            try {
                CSVParser csvRecords = new CSVParser(new StringReader(row), CSVFormat.DEFAULT.withSkipHeaderRecord());
                org.apache.commons.csv.CSVRecord next = csvRecords.iterator().next();
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
                                                  long offset, String fileName)
                throws Exception {
            ObjectNode node = mapper.createObjectNode();
            for (String key : headerNameToIndexMap.keySet()) {
                int index = headerNameToIndexMap.get(key);
                String value = record.get(index);
                node.put(key, value);
            }
            node.put("bicycle.offset", offset);
            node.put("bicycle.filename", fileName);
            JsonRawEvent jsonRawEvent = connector.createJsonRawEvent(node);
            return jsonRawEvent;
        }
    }

    public static class FileRecordOffset {

        private String fileName;
        private long offset;
        public FileRecordOffset(String fileName, long offset) {
            this.fileName = fileName;
            this.offset = offset;
        }

    }


}
