package io.airbyte.integrations.bicycle.base.integration;

import com.inception.server.auth.api.SystemAuthenticator;
import io.airbyte.integrations.bicycle.base.integration.csv.CSVEventSourceReader;
import io.airbyte.integrations.bicycle.base.integration.csv.CSVEventSourceReaderV2;
import io.bicycle.event.processor.impl.BicycleEventProcessorImpl;
import io.bicycle.integration.common.Status;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.server.event.mapping.UserServiceMappingRule;
import io.bicycle.server.event.mapping.constants.BicycleEventPublisherType;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import java.nio.charset.Charset;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
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
                            (fileName) -> getCSVReader(fileName, files.get(fileName), getConnectorId(), this, READ));
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
            reader = getCSVReader(fileName, csvFile, getConnectorId(), this, READ);
            List<RawEvent> invalidEvents = new ArrayList<>();
            while (reader.hasNext()) {
                RawEvent next = reader.next();
                if (reader.isValidEvent()) {
                    try {
                        long timestampInMillis = reader.getRecordUTCTimestampInMillis();
                        timestampToFileOffsetsMap.computeIfAbsent(timestampInMillis,
                                (recordOffset) -> new ArrayList<>()).add(new FileRecordOffset(fileName, reader.getOffset(), reader.getRowCounter()));
                        successCounter.incrementAndGet();
                        if (successCounter.get() % 1000 == 0) {
                            LOGGER.info("[{}] : Processed records by timestamp [{}] [{}]", getConnectorId(),
                                    successCounter.get(), timestampInMillis);
                        }
                    } catch (Exception e) {
                        LOGGER.error("Skipped record row[{}] offset[{}]", reader.getRow(), reader.getRowCounter(), e);
                        failedCounter.incrementAndGet();
                        invalidEvents.add(next);
                    }
                } else {
                    LOGGER.info("Skipped record row[{}] offset[{}]", reader.getRow(), reader.getRowCounter());
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

    protected List<UserServiceMappingRule> getUserServiceMappingRules() {
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

    protected CSVEventSourceReader getCSVReader(String fileName, File file, String connectorId,
                                                BaseEventConnector connector,
                                                APITYPE apitype) {
        if (System.getenv("ENABLE_CSV_READER_V1") != null) {
            return new CSVEventSourceReader(fileName, file, connectorId, connector, apitype);
        } else {
            return new CSVEventSourceReaderV2(fileName, file, connectorId, connector, apitype);
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
