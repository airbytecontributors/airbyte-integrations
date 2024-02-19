package io.airbyte.integrations.bicycle.base.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inception.common.client.ServiceLocator;
import com.inception.common.client.impl.GenericApiClient;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import com.inception.tenant.client.TenantServiceAPIClient;
import com.inception.tenant.query.TenantInfo;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.bicycle.ai.model.tenant.summary.discovery.*;
import io.bicycle.blob.store.client.BlobStoreApiClient;
import io.bicycle.blob.store.client.BlobStoreClient;
import io.bicycle.blob.store.schema.BlobObject;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.integration.common.config.ConnectorConfigService;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.integration.common.services.config.ConnectorConfigServiceImpl;
import io.bicycle.integration.common.utils.BlobStoreBroker;
import io.bicycle.integration.common.utils.CommonUtil;
import io.bicycle.integration.connector.ConfiguredConnectorStream;
import io.bicycle.preview.store.PreviewStoreClient;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import io.bicycle.server.sources.mapping.utils.PreviewEventSamplingHandler;
import io.bicycle.server.verticalcontext.tenant.api.Source;
import io.bicycle.server.verticalcontext.tenant.api.VerticalIdentifier;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.*;

import static io.bicycle.utils.metric.MetricNameConstants.PREVIEW_EVENTS_PUBLISH_NETWORK_CALL_TIME;

public abstract class BaseCSVEventConnector extends BaseEventConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseCSVEventConnector.class);
    private static final int BATCH_SIZE = 30;

    public BaseCSVEventConnector(SystemAuthenticator systemAuthenticator,
                                 EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier,
                                 ConnectorConfigManager connectorConfigManager) {
        super(systemAuthenticator, eventConnectorJobStatusNotifier, connectorConfigManager);
    }

    protected long updateFilesMetadata(File csvFile, List<RawEvent> vcEvents, int recordsCount, boolean updateVC) throws IOException {
        String streamId = getConnectorId();
        RandomAccessFile accessFile = null;
        String row = null;
        long counter = 0;
        long nullRows = 0;
        try {
            accessFile = new RandomAccessFile(csvFile, "r");
            String headersLine = accessFile.readLine();
//            bytesRead += headersLine.getBytes().length;
            Map<String, Integer> headerNameToIndexMap = getHeaderNameToIndexMap(headersLine);
            if (headerNameToIndexMap.isEmpty()) {
                throw new RuntimeException("Unable to read headers from csv file " + csvFile + " for " + streamId);
            }
            List<RawEvent> rawEvents = new ArrayList<>();
            List<RawEvent> sanityRawEvents = new ArrayList<>();
            do {
                try {
                    long offset = accessFile.getFilePointer();
                    row = accessFile.readLine();
                    if (!StringUtils.isEmpty(row)) {
                        CSVRecord csvRecord = getCsvRecord(offset, row, headerNameToIndexMap);
                        nullRows = 0;
                        if (csvRecord == null) {
                            continue;
                        }
                        RawEvent rawEvent = convertRecordsToRawEvents(headerNameToIndexMap, csvRecord);
                        if (headerNameToIndexMap.size() != csvRecord.size()) {
                            sanityRawEvents.add(rawEvent);
                            continue;
                        } else {
                            rawEvents.add(rawEvent);
                        }
                        if (sanityRawEvents.size() >= BATCH_SIZE) {
                            submitRecordsToPreviewStore(getTenantId(), sanityRawEvents, true);
                            sanityRawEvents.clear();
                        }
                        if (rawEvents.size() >= BATCH_SIZE) {
                            submitRecordsToPreviewStore(getConnectorId(), rawEvents, false);
                            if (updateVC) {
                                vcEvents.addAll(rawEvents);
                            }
                            rawEvents.clear();
                        }
                        if (counter > recordsCount) {
                            if (sanityRawEvents.size() > 0) {
                                submitRecordsToPreviewStore(getConnectorId()+"-sanity", sanityRawEvents, true);
                                sanityRawEvents.clear();
                            }
                            if (rawEvents.size() > 0) {
                                submitRecordsToPreviewStore(getConnectorId(), rawEvents, false);
                                rawEvents.clear();
                            }
                            return counter;
                        }
                    } else if (StringUtils.isEmpty(row)) {
                        nullRows++;
                        continue;
                    } else {
                        LOGGER.info("Exiting as coming in else block for stream Id {} and counter is at {}",
                                streamId, counter);
                        break;
                    }
                    counter++;
                    //just for logging progress
                    if (counter % 10000 == 0) {
                        LOGGER.info("Processing the file for stream Id {} and counter is at {}", streamId, counter);
                    }

                } catch (Exception e) {
                    LOGGER.error("Error while calculating timestamp to offset map for a row [{}] for stream Id [{}]",
                            row, streamId, e);
                }
            } while (nullRows <= 5000); // this is assuming once there are consecutive 100 null rows file read is complete.

        } catch (Exception e) {
            LOGGER.error("Error while calculating timestamp to offset map", e);
        } finally {
            if (accessFile != null) {
                accessFile.close();
            }
        }
        LOGGER.info("Total rows processed for file {} with stream Id {} is {}", csvFile, streamId, counter);
        return counter;
    }

    /*public static class CSVEventSourceReader extends EventSourceReader {

        private String connectorId;
        private File csvFile;

        RandomAccessFile accessFile = null;
        Map<String, Integer> headerNameToIndexMap;

        public CSVEventSourceReader(File csvFile, String connectorId) {
            this.connectorId = connectorId;
            this.csvFile = csvFile;
            initialize();
        }


        private void initialize() {
            try {
                accessFile = new RandomAccessFile(csvFile, "r");
                String headersLine = accessFile.readLine();
    //          bytesRead += headersLine.getBytes().length;
                Map<String, Integer> headerNameToIndexMap = getHeaderNameToIndexMap(headersLine);
                if (headerNameToIndexMap.isEmpty()) {
                    throw new RuntimeException("Unable to read headers from csv file " + csvFile + " for " + connectorId);
                }
            } catch (Exception e) {
                throw new IllegalStateException("Failed to read csv file ["+csvFile+"]", e);
            }
        }

        public boolean hasNext() {
            return false;
        }

        public void next() {
            try {
                long offset = accessFile.getFilePointer();
                String row = accessFile.readLine();
                if (!StringUtils.isEmpty(row)) {
                    CSVRecord csvRecord = getCsvRecord(offset, row, headerNameToIndexMap);
                    if (csvRecord == null) {
                        continue;
                    }
                    RawEvent rawEvent = convertRecordsToRawEvents(headerNameToIndexMap, csvRecord);
                    if (headerNameToIndexMap.size() != csvRecord.size()) {
                        sanityRawEvents.add(rawEvent);
                        continue;
                    } else {
                        rawEvents.add(rawEvent);
                    }
                    if (sanityRawEvents.size() >= BATCH_SIZE) {
                        submitRecordsToPreviewStore(getTenantId(), sanityRawEvents, true);
                        sanityRawEvents.clear();
                    }
                    if (rawEvents.size() >= BATCH_SIZE) {
                        submitRecordsToPreviewStore(getConnectorId(), rawEvents, false);
                        if (updateVC) {
                            vcEvents.addAll(rawEvents);
                        }
                        rawEvents.clear();
                    }
                    if (counter > recordsCount) {
                        if (sanityRawEvents.size() > 0) {
                            submitRecordsToPreviewStore(getConnectorId()+"-sanity", sanityRawEvents, true);
                            sanityRawEvents.clear();
                        }
                        if (rawEvents.size() > 0) {
                            submitRecordsToPreviewStore(getConnectorId(), rawEvents, false);
                            rawEvents.clear();
                        }
                        return counter;
                    }
                } else if (StringUtils.isEmpty(row)) {
                    nullRows++;
                    continue;
                } else {
                    LOGGER.info("Exiting as coming in else block for stream Id {} and counter is at {}",
                            streamId, counter);
                    break;
                }
                counter++;
                //just for logging progress
                if (counter % 10000 == 0) {
                    LOGGER.info("Processing the file for stream Id {} and counter is at {}", streamId, counter);
                }

            } catch (Exception e) {
                LOGGER.error("Error while calculating timestamp to offset map for a row [{}] for stream Id [{}]",
                        row, streamId, e);
            }
        }

        public void close() throws Exception {
            if (accessFile != null) {
                accessFile.close();
            }
        }
    }*/

    /*protected Map<Long, List<FileRecordOffset>> updateFilesMetadataOffsets(Map<Long, List<FileRecordOffset>> timestampToFileOffsetMap,
                                                                    List<RawEvent> rawEvents,
                                                                    String fileName, File csvFile) throws IOException {
        String streamId = getConnectorId();
        RandomAccessFile accessFile = null;
        String row = null;
        long counter = 0;
        long nullRows = 0;
        try {
            accessFile = new RandomAccessFile(csvFile, "r");
            String headersLine = accessFile.readLine();
//            bytesRead += headersLine.getBytes().length;
            headerNameToIndexMap = getHeaderNameToIndexMap(headersLine);
            if (headerNameToIndexMap.isEmpty()) {
                throw new RuntimeException("Unable to read headers from csv file " + csvFile + " for " + streamId);
            }

            do {
                try {
                    long offset = accessFile.getFilePointer();
                    row = accessFile.readLine();
                    if (!StringUtils.isEmpty(row)) {
                        CSVRecord csvRecord = getCsvRecord(offset, row, headerNameToIndexMap);
                        nullRows = 0;
                        if (csvRecord == null) {
                            continue;
                        }
                        int dateTimeFieldIndex = headerNameToIndexMap.get(dateTimeFieldColumnName);
                        String timestampFieldValue = csvRecord.get(dateTimeFieldIndex);
                        long timestampInMillisInEpoch = convertStringToTimestamp(timestampFieldValue);
                        timestampToFileOffsetMap.computeIfAbsent(timestampInMillisInEpoch,
                                (recordOffset) -> new ArrayList<>()).add(new FileRecordOffset(fileName, offset));
                        RawEvent rawEvent = convertRecordsToRawEvents(csvRecord);
                        rawEvents.add(rawEvent);
                    } else if (StringUtils.isEmpty(row)) {
                        nullRows++;
                        continue;
                    } else {
                        LOGGER.info("Exiting as coming in else block for stream Id {} and counter is at {}",
                                streamId, counter);
                        break;
                    }
                    counter++;
                    //just for logging progress
                    if (counter % 10000 == 0) {
                        LOGGER.info("Processing the file for stream Id {} and counter is at {}", streamId, counter);
                    }

                } catch (Exception e) {
                    LOGGER.error("Error while calculating timestamp to offset map for a row [{}] for stream Id [{}]",
                            row, streamId, e);
                }
            } while (nullRows <= 5000); // this is assuming once there are consecutive 100 null rows file read is complete.

        } catch (Exception e) {
            LOGGER.error("Error while calculating timestamp to offset map", e);
        } finally {
            if (accessFile != null) {
                accessFile.close();
            }
        }
        LOGGER.info("Total rows processed for file {} with stream Id {} is {}", csvFile, streamId, counter);
    }*/

    private CSVRecord getCsvRecord(long offset, String row, Map<String, Integer> headerNameToIndexMap) {
        String streamId = getConnectorId();
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

    protected void processFile(Map<String, String> fileNameVsSignedUrl) {
        Map<String, File> fileNameVsLocalFiles = new HashMap<>();
        for (String fileName : fileNameVsSignedUrl.keySet()) {
            String signedUrl = fileNameVsSignedUrl.get(fileName);
            File file = storeFile(fileName, signedUrl);
            fileNameVsLocalFiles.put(fileName, file);
        }
    }

    public RawEvent convertRecordsToRawEvents(Map<String, Integer> headerNameToIndexMap, CSVRecord record) throws Exception {
        ObjectNode node = mapper.createObjectNode();
        for (String key : headerNameToIndexMap.keySet()) {
            int index = headerNameToIndexMap.get(key);
            String value = record.get(index);
            node.put(key, value);
        }
        JsonRawEvent jsonRawEvent = createJsonRawEvent(node);
        return jsonRawEvent;
    }

    /*private boolean storeRawEventsToPreviewStore(final String connectorId,
                                                 final AuthInfo authInfo,
                                                 final List<RawEvent> rawEvents,
                                                 final boolean shouldFlush) {
        if (rawEvents != null && rawEvents.isEmpty()) {
            return true;
        }
        if (!previewEventSamplingHandler.shouldPublishPreviewEvents(authInfo, connectorId)) {
            return false;
        }
        List<JsonNode> sampledRawEvents = previewEventSamplingHandler.getPreviewEventsAsJsonNodeArray(connectorId,
                authInfo, rawEvents);
        StoreRawJsonEventsRequest storeRawJsonEventsRequest = this.previewStoreRequestResponseUtils
                .buildStoreRawJsonEventsRequest(connectorId, sampledRawEvents, shouldFlush);
        logger.debug("Attempting to store raw json events to preview store for connectorId {}", connectorId);
        Timer.Context previewEventsPublishNetworkCallTime = MetricUtils.getMetricRegistry().timer(
                PREVIEW_EVENTS_PUBLISH_NETWORK_CALL_TIME
                        .withTags(SOURCE_ID, connectorId)
                        .toString()
        ).time();

        StoreRawEventsResponse storeRawEventsResponse =
                this.previewStoreClient.storeRawJsonEvents(authInfo, storeRawJsonEventsRequest);
        previewEventsPublishNetworkCallTime.stop();
        logger.info("Stored raw json events in preview events for connectorId {}, response {}",
                connectorId, storeRawEventsResponse);
        return true;
    }*/

    public static class FileRecordOffset {

        private String fileName;
        private long offset;
        public FileRecordOffset(String fileName, long offset) {
            this.fileName = fileName;
            this.offset = offset;
        }

    }


}
