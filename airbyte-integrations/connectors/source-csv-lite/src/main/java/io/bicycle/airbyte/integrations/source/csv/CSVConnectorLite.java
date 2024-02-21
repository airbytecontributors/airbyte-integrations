package io.bicycle.airbyte.integrations.source.csv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.integrations.bicycle.base.integration.BaseCSVEventConnector;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.*;
import io.bicycle.integration.common.Status;
import io.bicycle.integration.common.StatusResponse;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.integration.connector.SyncDataRequest;
import io.bicycle.integration.connector.SyncDataResponse;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author <a href="mailto:ravi.noothi@agilitix.ai">Ravi Kiran Noothi</a>
 * @since 14/11/22
 */

public class CSVConnectorLite extends BaseCSVEventConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVConnectorLite.class);

    private static final String SEPARATOR_CHAR = ",";

    private static final int PREVIEW_RECORDS = 100;
    private static final String SYNC_DATA_STATE = "SYNC_DATA_STATE";
    private static final String TOTAL_RECORDS = "TOTAL_RECORDS";
    private static final String PREVIEW_STORE_VALID_RECORDS = "PREVIEW_STORE_VALID_RECORDS";
    private static final String PREVIEW_STORE_INVALID_RECORDS = "PREVIEW_STORE_INVALID_RECORDS";
    private static final String STARTED = "STARTED";
    private static final String IN_PROGRESS = "IN_PROGRESS";
    private static final String FINISHED = "FINISHED";
    private static final String FAILED = "FAILED";

    private volatile boolean shutdown = false;

    private File file;
    private String[] headers;

    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private ObjectMapper mapper = new ObjectMapper();

    public CSVConnectorLite(SystemAuthenticator systemAuthenticator,
                        EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier,
                        ConnectorConfigManager connectorConfigManager) {
        super(systemAuthenticator, eventConnectorJobStatusNotifier, connectorConfigManager);
    }

    protected int getTotalRecordsConsumed() {
        return 0;
    }

    public void stopEventConnector() {
        shutdown = true;
        stopEventConnector("Successfully Stopped", JobExecutionStatus.success);
    }

    public AutoCloseableIterator<AirbyteMessage> preview(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) throws InterruptedException, ExecutionException {
        return null;
    }

    public SyncDataResponse syncData(JsonNode sourceConfig, ConfiguredAirbyteCatalog catalog,
                                     JsonNode readState, SyncDataRequest syncDataRequest) {
        initialize(sourceConfig, catalog);
        Status syncState = null;
        try {
            syncState = getConnectorSyncStatus();
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Failed to fetch the sync state");
        }
        if (syncState != null) {
            return SyncDataResponse.getDefaultInstance();
        }
        Map<String, String> fileVsSignedUrls = readFilesConfig();
        LOGGER.info("[{}] : Signed files Url [{}]", getConnectorId(), fileVsSignedUrls);
        Map<String, File> files = new HashMap<>();
        for (String fileName : fileVsSignedUrls.keySet()) {
            File file = storeFile(fileName, fileVsSignedUrls.get(fileName));
            files.put(fileName, file);
        }
        //files.put("test", new File("/home/ravi/Downloads/test.csv"));
        LOGGER.info("[{}] : Local files Url [{}]", getConnectorId(), files);
        List<RawEvent> vcEvents = new ArrayList<>();
        for (String fileName : files.keySet()) {
            File file = files.get(fileName);
            CSVEventSourceReader csvReader = null;
            try {
                csvReader = null;
                try {
                    csvReader = new CSVEventSourceReader(fileName, file, getConnectorId(), this);
                    publishPreviewEvents(file, csvReader, vcEvents, PREVIEW_RECORDS, 1,
                            false, true, true);
                } finally {
                    if (csvReader != null) {
                        csvReader.close();
                    }
                }
            } catch (Throwable t) {
                throw new IllegalStateException("Failed to register preview events for discovery service ["+fileName+"]", t);
            }
        }
        //createTenantVC(vcEvents);
        try {
            Future<Object> future = processFiles(files);
            //future.get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        return SyncDataResponse.newBuilder()
                .setStatus(Status.COMPLETE)
                .setResponse(StatusResponse.newBuilder().setMessage("SUCCESS").build())
                .build();
    }

    private Future<Object> processFiles(Map<String, File> files)
            throws JsonProcessingException, InvalidProtocolBufferException {
        updateConnectorSyncState(Status.STARTED, 0);
        BaseEventConnector connector = this;
        Future<Object> future = executorService.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    updateConnectorSyncState(Status.IN_PROGRESS, 0);
                    int total_records = 0;
                    for (String fileName : files.keySet()) {
                        File file = files.get(fileName);
                        int records = totalRecords(file);
                        records = records - 1; //remove header
                        total_records = total_records + records;
                    }
                    for (String fileName : files.keySet()) {
                        File file = files.get(fileName);
                        CSVEventSourceReader csvReader = null;
                        try {
                            csvReader = new CSVEventSourceReader(fileName, file, getConnectorId(), connector);
                            publishPreviewEvents(file, csvReader, Collections.emptyList(),
                                    Integer.MAX_VALUE, total_records,
                                    true, false, false);
                        } finally {
                            if (csvReader != null) {
                                csvReader.close();
                            }
                        }
                    }
                    LOGGER.info("[{}] : Raw events total - Total Count[{}]", getConnectorId(), total_records);
                    updateConnectorSyncState(Status.COMPLETE);
                } catch (Throwable t) {
                    LOGGER.error("Failed to submit records to preview store [{}]", getConnectorId(), t);
                    updateConnectorSyncState(Status.ERROR);
                }
                return null;
            }
        });
        return future;
    }

    public AirbyteConnectionStatus check(JsonNode config) throws Exception {
        LOGGER.info("Check the status");
        return new AirbyteConnectionStatus()
                .withStatus(AirbyteConnectionStatus.Status.SUCCEEDED)
                .withMessage("Success");
    }

    public AirbyteCatalog discover(JsonNode config) throws Exception {
        LOGGER.info("Discover the csv");
        String datasetName = null;
        if (getDatasetName(config) != null) {
            datasetName = getDatasetName(config);
        } else {
            throw new IllegalStateException("No dataset name is set");
        }
        final List<AirbyteStream> streams = Collections.singletonList(
                CatalogHelpers.createAirbyteStream(datasetName, Field.of("value", JsonSchemaType.STRING))
                        .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
        );
        return new AirbyteCatalog().withStreams(streams);
    }

    public AutoCloseableIterator<AirbyteMessage> doRead(
            JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state){
        String connectorState = getStateAsString(CONNECTOR_STATE);
        if (connectorState != null && connectorState.equalsIgnoreCase("DONE")) {
            return null;
        }
        EventSourceInfo eventSourceInfo = new EventSourceInfo(getConnectorId(), getEventSourceType());
        boolean doesMappingRulesExist = doesMappingRulesExists(getAuthInfo(), eventSourceInfo);

        Map<String, String> fileVsSignedUrls = readFilesConfig();
        LOGGER.info("[{}] : Signed files Url [{}]", getConnectorId(), fileVsSignedUrls);
        Map<String, File> files = new HashMap<>();
        for (String fileName : fileVsSignedUrls.keySet()) {
            File file = storeFile(fileName, fileVsSignedUrls.get(fileName));
            files.put(fileName, file);
            files.put(fileName, new File("/home/ravi/Downloads/test.csv"));
        }

        Map<Long, List<FileRecordOffset>> timestampToFileOffsetsMap = new HashMap<>();
        for (String fileName : files.keySet()) {
            File file = files.get(fileName);
            try {
                readTimestampToFileOffset(timestampToFileOffsetsMap, fileName, file);
            } catch (Throwable t) {
                throw new IllegalStateException("Failed to register preview events for discovery service ["+fileName+"]", t);
            }
        }
        try {
            processCSVFile(timestampToFileOffsetsMap, files);
            saveState(CONNECTOR_STATE, "DONE");
        } catch (IOException e) {
            try {
                saveState(CONNECTOR_STATE, "FAILED");
            } catch (JsonProcessingException ex) {
            }
            throw new IllegalStateException(e);
        }
        return null;
    }

    private CSVRecord getCsvRecord(long recordOffset, RandomAccessFile accessFile) throws Exception {
        accessFile.seek(recordOffset);
        String row = accessFile.readLine();
        CSVRecord csvRecord = getCsvRecord(recordOffset, row);
        return csvRecord;
    }

    public List<RawEvent> convertRecordsToRawEventsInternal(List<?> records) {
        return null;
    }

    private String getCsvUrl(JsonNode config) {
        return config.get("url") != null ? config.get("url").asText() : null;
    }

    private String getDatasetName(JsonNode config) {
        return config.get("datasetName") != null ? config.get("datasetName").asText() : null;
    }

    private CSVRecord getCsvRecord(long offset, String row) {
        try {
            String[] columns = sanitize(row);
            if (columns != null && columns.length == 0) {
                LOGGER.warn("Ignoring the row");
                return null;
            } else if (columns == null || headers.length != columns.length) {
                LOGGER.error("Headers and Columns do not match ["+Arrays.asList(headers)
                        +"] ["+Arrays.asList(columns)+"]");
                return null;
            }
            CSVRecord record = new CSVRecord(headers, columns, offset);
            return record;
        } catch (Throwable e) {
            LOGGER.error("Failed to parse the row [{}] [{}]. Row will be ignored", offset, row, e);
            System.out.println("Ignored row ["+offset+"] ["+row+"]");
            e.printStackTrace();
        }
        return null;
    }

  /*  private String[] sanitize(String row) {
        String[] values = new String[0];
        if (values != null) {
            for (int i=0; i < values.length; i++) {
                String value = values[i];
                if (value.startsWith("\"") || value.startsWith("\'")) {
                    value = value.substring(1);
                }
                if (value.endsWith("\"") || value.endsWith("\'")) {
                    value = value.substring(0, value.length() - 1);
                }
                values[i] = value;
            }
        }
        return values;
    }*/

    private static String[] sanitize(String row) {

        try {
            CSVParser csvRecords = new CSVParser(new StringReader(row), CSVFormat.DEFAULT.withSkipHeaderRecord());
            org.apache.commons.csv.CSVRecord next = csvRecords.iterator().next();
            Iterator<String> iterator = next.iterator();
            List<String> values = new ArrayList<>();
            while (iterator.hasNext()) {
                values.add(iterator.next());
            }
            return values.toArray(new String[]{});
        } catch (Exception e) {
            LOGGER.error("Failed to parse the row using csv reader " + row, e);
            return new String[0];
        }

    }

    private String[] readRecord(File file, long offset) throws IOException {
        RandomAccessFile accessFile = new RandomAccessFile(file, "r");
        try {
            accessFile.seek(offset);
            String row = accessFile.readLine();
            if (row != null && !row.isEmpty()) {
                String[] columns = row.split(SEPARATOR_CHAR);
                return columns;
            }
        } finally {
            if (accessFile != null)
                accessFile.close();
        }
        return null;
    }

    class CSVRecord {

        private  Map<String, String> map;
        private long offset;
        public CSVRecord(String[] headers, String[] columns, long offset) {
            this.offset = offset;
            map = new HashMap<>();
            int i = 0;
            for (String header : headers) {
                map.put(header.trim(), columns[i].trim());
                i++;
            }
        }

        public Map<String, String> toMap() {
            return map;
        }
    }
}
