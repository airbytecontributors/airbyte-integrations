package io.airbyte.integrations.bicycle.base.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inception.server.auth.api.SystemAuthenticator;
import io.bicycle.entity.mapping.SourceFieldMapping;
import io.bicycle.event.publisher.api.MetadataPreviewEventType;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.server.event.mapping.UserServiceFieldDef;
import io.bicycle.server.event.mapping.UserServiceFieldsList;
import io.bicycle.server.event.mapping.UserServiceFieldsRule;
import io.bicycle.server.event.mapping.UserServiceMappingRule;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import java.nio.charset.Charset;
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

    protected static final String PROCESS_TIMESTAMP = "PROCESS_TIMESTAMP";
    protected static final String CONNECTOR_STATE = "CONNECTOR_STATE";

    public BaseCSVEventConnector(SystemAuthenticator systemAuthenticator,
                                 EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier,
                                 ConnectorConfigManager connectorConfigManager) {
        super(systemAuthenticator, eventConnectorJobStatusNotifier, connectorConfigManager);
    }

    protected int totalRecords(File file) {
        try (Reader reader = new FileReader(file, Charset.defaultCharset());
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

    protected void processCSVFile(Map<Long, List<FileRecordOffset>> timestampToFileOffsetsMap, Map<String, File> files)
                                throws IOException {
        try {
            long maxTimestamp = getStateAsLong(PROCESS_TIMESTAMP);
            long recordsProcessed = 0;
            List<RawEvent> rawEvents = new ArrayList<>();
            Map<String, CSVEventSourceReader> readers = new HashMap<>();
            long timestamp = 0;
            for (Map.Entry<Long, List<FileRecordOffset>> entry: timestampToFileOffsetsMap.entrySet()) {
                timestamp = entry.getKey();
                if (timestamp < maxTimestamp) {
                    LOGGER.warn("Ignoring events for timestamp {} either because its less than state or " +
                            "doesn't fall in backfill start time and backfill end time", timestamp);
                    continue;
                }
                List<FileRecordOffset> fileRecordOffsets = entry.getValue();
                for (FileRecordOffset fileRecordOffset: fileRecordOffsets) {
                    CSVEventSourceReader reader = readers.computeIfAbsent(fileRecordOffset.fileName,
                            (fileName) -> new CSVEventSourceReader(fileName, files.get(fileName), getConnectorId(), this));
                    long offset = fileRecordOffset.offset;
                    reader.seek(offset);
                    RawEvent next = reader.next();
                    rawEvents.add(next);
                    recordsProcessed++;
                }

                if (rawEvents.size() >= BATCH_SIZE) {
                    boolean success = processAndPublishEvents(rawEvents);
                    if (success) {
                        saveState(PROCESS_TIMESTAMP, timestamp);
                    }
                }
            }

            if (rawEvents.size() > 0) {
                boolean success = processAndPublishEvents(rawEvents);
                if (success && timestamp > 0) {
                    saveState(PROCESS_TIMESTAMP, timestamp);
                }
            }

            for (String fileName : readers.keySet()) {
                readers.get(fileName).close();
            }

            LOGGER.info("Total records processed for stream {} and file name {} is {} with max timestamp {}", getConnectorId(),
                    recordsProcessed, timestamp);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Map<Long, List<FileRecordOffset>> readTimestampToFileOffset(Map<Long, List<FileRecordOffset>> timestampToFileOffsetsMap,
                                                                        String fileName, File csvFile) throws Exception {
        CSVEventSourceReader reader = null;
        try {
            reader = new CSVEventSourceReader(fileName, csvFile, getConnectorId(), this);
            while (reader.hasNext()) {
                reader.next();
                long timestampInMillis = reader.getRecordUTCTimestampInMillis();
                timestampToFileOffsetsMap.computeIfAbsent(timestampInMillis,
                        (recordOffset) -> new ArrayList<>()).add(new FileRecordOffset(fileName, reader.offset));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Error while calculating timestamp to offset map ["+fileName+"]", e);
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        return timestampToFileOffsetsMap;
    }

    public static class CSVEventSourceReader extends EventSourceReader<RawEvent> {

        private String connectorId;
        private File csvFile;
        RandomAccessFile accessFile = null;
        Map<String, Integer> headerNameToIndexMap;
        private boolean validEvent = false;
        private RawEvent nextEvent;
        private CSVRecord csvRecord;
        private long offset = -1;
        private BaseEventConnector connector;

        protected ObjectMapper mapper = new ObjectMapper();

        private String name;
        private String row = null;
        private long counter = 0;
        private long nullRows = 0;

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
            validEvent = true;
            row = null;
            offset = -1;
            nextEvent = null;
            csvRecord = null;
        }

        public void seek(long offset) throws IOException {
            this.accessFile.seek(offset);
            this.row = accessFile.readLine();
            this.offset = offset;
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

        public long getRecordUTCTimestampInMillis() {
            UserServiceFieldDef startTimeFieldDef = null;
            List<UserServiceMappingRule> userServiceMappingRules =
                    connector.getUserServiceMappingRules(connector.getAuthInfo(), connector.eventSourceInfo);
            boolean found = false;
            for (UserServiceMappingRule userServiceMappingRule : userServiceMappingRules) {
                UserServiceFieldsRule userServiceFields = userServiceMappingRule.getUserServiceFields();
                List<UserServiceFieldDef> commonFieldsList = userServiceFields.getCommonFieldsList();
                for (UserServiceFieldDef userServiceFieldDef : commonFieldsList) {
                    if (userServiceFieldDef.getPredefinedFieldType().equals("startTimeMicros")) {
                        startTimeFieldDef = userServiceFieldDef;
                        found = true;
                        break;
                    }
                }
                if (found) {
                    break;
                }

                Map<String, UserServiceFieldsList> userServiceFieldsMap = userServiceFields.getUserServiceFieldsMap();
                for (String key : userServiceFieldsMap.keySet()) {
                    UserServiceFieldsList userServiceFieldsList = userServiceFieldsMap.get(key);
                    for (UserServiceFieldDef userServiceFieldDef : userServiceFieldsList.getUserServiceFieldList()) {
                        if (userServiceFieldDef.getPredefinedFieldType().equals("startTimeMicros")) {
                            startTimeFieldDef = userServiceFieldDef;
                            found = true;
                            break;
                        }
                    }
                    if (found) {
                        break;
                    }
                }
                if (found) {
                    break;
                }
            }
            if (startTimeFieldDef == null) {
                throw new IllegalStateException("timestamp field is not discovered yet");
            }

            SourceFieldMapping fieldMapping = startTimeFieldDef.getFieldMapping();
            long valueInMicros = (long) nextEvent.getFieldValue(fieldMapping, Collections.emptyMap(), connector.getAuthInfo());
            return valueInMicros / 1000;
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

        public boolean isValidEvent() {
            return validEvent;
        }

        public RawEvent next() {
            try {
                counter++;
                csvRecord = getCsvRecord(offset, row, headerNameToIndexMap);
                if (csvRecord != null) {
                    nextEvent = convertRecordsToRawEvents(headerNameToIndexMap, csvRecord, offset, name);
                    return nextEvent;
                }
            } catch (Exception e) {
                validEvent = false;
                LOGGER.error("Failed Parsing ["+row+"] ["+offset+"]", e);
            }
            ObjectNode node = mapper.createObjectNode();
            node.put("bicycle.raw.event.record", row);
            nextEvent = getJsonRawEvent(offset, name, node);
            return nextEvent;
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
            if (headerNameToIndexMap.size() != record.size()) {
                validEvent = false;
            } else {
                validEvent = true;
            }
            if (validEvent) {
                for (String key : headerNameToIndexMap.keySet()) {
                    int index = headerNameToIndexMap.get(key);
                    String value = record.get(index);
                    node.put(key, value);
                }
            } else {
                node.put("bicycle.raw.event.record", record.toString());
            }
            JsonRawEvent jsonRawEvent = getJsonRawEvent(offset, fileName, node);
            return jsonRawEvent;
        }

        private JsonRawEvent getJsonRawEvent(long offset, String fileName, ObjectNode node) {
            node.put("bicycle.raw.event.identifier", String.valueOf(offset));
            if (!validEvent) {
                node.put("bicycle.metadata.eventType", MetadataPreviewEventType.SYNC_ERROR.name());
            }
            node.put("bicycle.eventSourceId", connectorId);
            node.put("bicycle.raw.event.error", "");
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
