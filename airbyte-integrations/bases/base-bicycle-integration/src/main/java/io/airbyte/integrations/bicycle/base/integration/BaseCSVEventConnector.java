package io.airbyte.integrations.bicycle.base.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inception.server.auth.api.SystemAuthenticator;
import io.airbyte.integrations.bicycle.base.integration.exception.UnsupportedFormatException;
import io.bicycle.entity.mapping.SourceFieldMapping;
import io.bicycle.event.processor.impl.BicycleEventProcessorImpl;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.integration.common.Status;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.server.event.mapping.UserServiceFieldDef;
import io.bicycle.server.event.mapping.UserServiceFieldsList;
import io.bicycle.server.event.mapping.UserServiceFieldsRule;
import io.bicycle.server.event.mapping.UserServiceMappingRule;
import io.bicycle.server.event.mapping.api.MetadataPreviewEventType;
import io.bicycle.server.event.mapping.constants.BicycleEventPublisherType;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import static io.airbyte.integrations.bicycle.base.integration.BaseCSVEventConnector.APITYPE.READ;

public abstract class BaseCSVEventConnector extends BaseEventConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseCSVEventConnector.class);

    private List<UserServiceMappingRule> userserviceRules = null;

    protected static final String PROCESS_TIMESTAMP = "PROCESS_TIMESTAMP";

    public BaseCSVEventConnector(SystemAuthenticator systemAuthenticator,
                                 EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier,
                                 ConnectorConfigManager connectorConfigManager) {
        super(systemAuthenticator, eventConnectorJobStatusNotifier, connectorConfigManager);
    }

    protected void setBicycleEventProcessor() {
        this.bicycleEventProcessor =
                new BicycleEventProcessorImpl(
                        BicycleEventPublisherType.BICYCLE_EVENTS,
                        configStoreClient,
                        schemaStoreApiClient,
                        entityStoreApiClient,
                        previewStoreClient,
                        systemAuthenticator,
                        connectorConfigManager,
                        dataTransformer
                );
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

    protected long processCSVFile(int index, Map<Long, List<FileRecordOffset>> timestampToFileOffsetsMap, Map<String, File> files,
                                  long totalRecords, int batchSize, AtomicLong successCounter, AtomicLong failedCounter)
                                  throws IOException {
        int records = 0;
        try {
            LOGGER.info("Starting processing index[{}] [{}]", index, timestampToFileOffsetsMap.size());
            long start = System.currentTimeMillis();
            long maxTimestamp = getStateAsLong(PROCESS_TIMESTAMP);
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
                            (fileName) -> new CSVEventSourceReader(fileName, files.get(fileName), getConnectorId(), this, READ));
                    long offset = fileRecordOffset.offset;
                    long rowCounter = fileRecordOffset.rowCounter;
                    reader.seek(offset, rowCounter);
                    RawEvent next = reader.next();
                    rawEvents.add(next);
                    successCounter.incrementAndGet();
                    records++;
                }

                if (rawEvents.size() >= batchSize) {
                    int size = rawEvents.size();
                    boolean success = processAndPublishEventsWithRules(rawEvents);
                    rawEvents.clear();
                    if (success) {
                        saveState(PROCESS_TIMESTAMP, timestamp);
                        updateConnectorState(READ_STATUS, Status.IN_PROGRESS, (double) successCounter.get()/ (double) totalRecords);
                        LOGGER.info("[{}] : Success published records [{}] [{}] [{}]", getConnectorId(), index, records, successCounter.get());
                    } else {
                        failedCounter.addAndGet(size);
                        LOGGER.info("[{}] : Failed published records [{}] [{}]", getConnectorId(), index, failedCounter.get());
                    }
                }
            }

            if (rawEvents.size() > 0) {
                int size = rawEvents.size();
                boolean success = processAndPublishEventsWithRules(rawEvents);
                if (success && timestamp > 0) {
                    saveState(PROCESS_TIMESTAMP, timestamp);
                    updateConnectorState(READ_STATUS, Status.IN_PROGRESS, (double) successCounter.get()/ (double) totalRecords);
                    LOGGER.info("[{}] : Success published records [{}] [{}] [{}]", getConnectorId(), index, records, successCounter.get());
                } else {
                    failedCounter.addAndGet(size);
                    LOGGER.info("[{}] : Failed published records [{}] [{}]", getConnectorId(), index, failedCounter.get());
                }
            }

            for (String fileName : readers.keySet()) {
                readers.get(fileName).close();
            }

            LOGGER.info("Total records processed for stream {} records processed index {} success {} failed {} " +
                            "expected {} records {} total-records {} with max timestamp {} time {}",
                            getConnectorId(), index, successCounter.get(), failedCounter.get(),
                            timestampToFileOffsetsMap.size(), records, totalRecords, timestamp,
                            (System.currentTimeMillis() - start));
            updateConnectorState(READ_STATUS, Status.IN_PROGRESS, (double) successCounter.get()/ (double) totalRecords);
            return successCounter.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected long readTimestampToFileOffset(Map<Long, List<FileRecordOffset>> timestampToFileOffsetsMap,
                                             String fileName, File csvFile, int batchSize, AtomicLong successCounter,
                                             AtomicLong failedCounter)
                                             throws Exception {
        CSVEventSourceReader reader = null;
        try {
            long start = System.currentTimeMillis();
            reader = new CSVEventSourceReader(fileName, csvFile, getConnectorId(), this, READ);
            List<RawEvent> invalidEvents = new ArrayList<>();
            while (reader.hasNext()) {
                RawEvent next = reader.next();
                if (reader.isValidEvent()) {
                    try {
                        long timestampInMillis = reader.getRecordUTCTimestampInMillis();
                        timestampToFileOffsetsMap.computeIfAbsent(timestampInMillis,
                                (recordOffset) -> new ArrayList<>()).add(new FileRecordOffset(fileName, reader.offset, reader.rowCounter));
                        successCounter.incrementAndGet();
                        if (successCounter.get() % 1000 == 0) {
                            LOGGER.info("[{}] : Processed records by timestamp [{}] [{}]", getConnectorId(),
                                    successCounter.get(), timestampInMillis);
                        }
                    } catch (Exception e) {
                        LOGGER.error("Skipped record row[{}] offset[{}]", reader.row, reader.rowCounter, e);
                        failedCounter.incrementAndGet();
                        invalidEvents.add(next);
                    }
                } else {
                    LOGGER.info("Skipped record row[{}] offset[{}]", reader.row, reader.rowCounter);
                    failedCounter.incrementAndGet();
                    invalidEvents.add(next);
                }
                if (invalidEvents.size() >= batchSize) {
                    submitRecordsToPreviewStoreWithMetadata(getConnectorId(), invalidEvents);
                    invalidEvents.clear();
                }
            }
            if (invalidEvents.size() >= 0) {
                submitRecordsToPreviewStoreWithMetadata(getConnectorId(), invalidEvents);
                invalidEvents.clear();
            }
            LOGGER.info("[{}] : Total records processed [{}] [{}] [{}]", getConnectorId(), successCounter.get(),
                    failedCounter.get(), (System.currentTimeMillis() - start));
        } catch (Exception e) {
            throw new IllegalStateException("Error while calculating timestamp to offset map ["+fileName+"]", e);
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        return successCounter.get();
    }


    protected boolean processAndPublishEventsWithRules(List<RawEvent> rawEvents) {
        EventSourceInfo eventSourceInfo = new EventSourceInfo(getConnectorId(), getEventSourceType());
        EventProcessorResult eventProcessorResult = convertRawEventsToBicycleEvents(getAuthInfo(),
                eventSourceInfo, rawEvents, getUserServiceMappingRules());
        boolean publishEvents = true;
        publishEvents = publishEvents(getAuthInfo(), eventSourceInfo, eventProcessorResult);
        return publishEvents;
    }

    private List<UserServiceMappingRule> getUserServiceMappingRules() {
        if (userserviceRules != null) {
            return userserviceRules;
        }
        int retries = 0;
        do {
            try {
                userserviceRules = getUserServiceMappingRules(getAuthInfo(), eventSourceInfo);
                if (userserviceRules != null) {
                    break;
                }
                Thread.sleep(500);
            } catch (Throwable t) {
                LOGGER.error("Failed to download us rules[{}]", getConnectorId(), t);
            }
            retries++;
        } while (userserviceRules == null && retries < 10);
        if (userserviceRules == null) {
            throw new IllegalStateException("Failed to download userservice userserviceRules ["+getConnectorId()+"]");
        }
        return userserviceRules;
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
        private long rowCounter = 0;
        private ReaderStatus status = ReaderStatus.SUCCESS;
        private BaseEventConnector connector;
        private APITYPE apiType;

        private SourceFieldMapping fieldMapping;

        protected ObjectMapper mapper = new ObjectMapper();

        private String name;
        private String row = null;
        private long counter = 0;
        private long nullRows = 0;

        public CSVEventSourceReader(String name, File csvFile, String connectorId,
                                    BaseEventConnector connector, APITYPE apiType) {
            this.name = name;
            this.connectorId = connectorId;
            this.csvFile = csvFile;
            this.connector = connector;
            this.apiType = apiType;
            initialize();
        }

        private void initialize() {
            try {
                accessFile = new RandomAccessFile(csvFile, "r");
                String headersLine = accessFile.readLine();
                rowCounter++;
                headerNameToIndexMap = getHeaderNameToIndexMap(headersLine);
                if (headerNameToIndexMap.isEmpty()) {
                    throw new RuntimeException("Unable to read headers from csv file " + csvFile + " for " + connectorId);
                }
            } catch (Exception e) {
                throw new IllegalStateException("Failed to read csv file ["+csvFile+"]", e);
            }
        }

        public void validateFileFormat() throws IOException, UnsupportedFormatException {
            RandomAccessFile accessFile = new RandomAccessFile(csvFile, "r");
            int count = 0;
            do {
                String line = accessFile.readLine();
                if (line != null && !line.contains(",")) {
                    throw new UnsupportedFormatException(csvFile.getName());
                }
                count++;
            } while (count < 3);
        }

        private void reset() {
            validEvent = true;
            row = null;
            offset = -1;
            nextEvent = null;
            csvRecord = null;
        }

        public void seek(long offset, long rowCounter) throws IOException {
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

        public long getRecordUTCTimestampInMillis() {
            SourceFieldMapping fieldMapping = getSourceFieldMapping();
            long valueInMicros = (long) nextEvent.getFieldValue(fieldMapping, Collections.emptyMap(), connector.getAuthInfo());
            return valueInMicros / 1000;
        }

        private SourceFieldMapping getSourceFieldMapping() {
            if (fieldMapping != null) {
                return fieldMapping;
            } else {
                UserServiceFieldDef startTimeFieldDef = null;
                List<UserServiceMappingRule> userServiceMappingRules =
                        connector.getUserServiceMappingRules(connector.getAuthInfo(), connector.getEventSourceInfo());
                LOGGER.info("[{}] : Userservice rules downloaded [{}]", connectorId, userServiceMappingRules);
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

                fieldMapping = startTimeFieldDef.getFieldMapping();
                return fieldMapping;
            }
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
                errorMessage = "["+e.getMessage() + "] ["+rowCounter+"] ["+row+"]";
                LOGGER.error("Failed Parsing ["+row+"] ["+rowCounter+"]", e);
            } finally {
                if (!isValidEvent()) {
                    status = ReaderStatus.FAILED;
                }
            }
            ObjectNode node = mapper.createObjectNode();
            node.put("bicycle.raw.event.record", row);
            nextEvent = getJsonRawEvent(rowCounter, name, node, errorMessage);
            return nextEvent;
        }

        public ReaderStatus getStatus() {
            return status;
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

        private JsonRawEvent getJsonRawEvent(long rowCounter, String fileName, ObjectNode node, String errorMessage) {
            node.put("bicycle.raw.event.identifier", String.valueOf(rowCounter));
            if (!validEvent) {
                if (apiType.equals(APITYPE.SYNC_DATA)) {
                    node.put("bicycle.metadata.eventType", MetadataPreviewEventType.SYNC_ERROR.name());
                } else if (apiType.equals(READ)) {
                    node.put("bicycle.metadata.eventType", MetadataPreviewEventType.READ_ERROR.name());
                }
            }
            node.put("bicycle.eventSourceId", connectorId);
            if (errorMessage != null) {
                node.put("bicycle.raw.event.error", errorMessage);
            }
            node.put("bicycle.filename", fileName);
            JsonRawEvent jsonRawEvent = connector.createJsonRawEvent(node);
            return jsonRawEvent;
        }
    }

    public enum APITYPE {
        SYNC_DATA,
        READ
    }

    public static class FileRecordOffset {

        private String fileName;
        private long offset;
        private long rowCounter;

        public FileRecordOffset(String fileName, long offset, long rowCounter) {
            this.fileName = fileName;
            this.offset = offset;
            this.rowCounter = rowCounter;
        }

    }


}