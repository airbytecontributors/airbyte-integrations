package io.bicycle.airbyte.integrations.source.csv;

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
import io.airbyte.integrations.bicycle.base.integration.exception.UnsupportedFormatException;
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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.airbyte.integrations.bicycle.base.integration.BaseCSVEventConnector.APITYPE.SYNC_DATA;

/**
 * @author <a href="mailto:ravi.noothi@agilitix.ai">Ravi Kiran Noothi</a>
 * @since 14/11/22
 */

public class CSVConnectorLite extends BaseCSVEventConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVConnectorLite.class);

    private static final String SEPARATOR_CHAR = ",";

    private static final int PREVIEW_RECORDS = 100;

    private volatile boolean shutdown = false;

    private File file;
    private String[] headers;

    private static AtomicLong threadcounter = new AtomicLong(0);
    private ExecutorService executorService;
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
        LOGGER.info("Starting syncdata [{}] [{}]", sourceConfig, readState);
        LOGGER.info("SyncData ConnectorConfigManager [{}]", connectorConfigManager);
        initialize(sourceConfig, catalog);
        Status syncStatus = null;
        try {
            syncStatus = getConnectorStatus(SYNC_STATUS);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Failed to fetch the sync state");
        }
        if (syncStatus != null) {
            LOGGER.info("Already preview ingesting records [{}] [{}]", getConnectorId(), syncStatus);
        }
        Map<String, String> fileVsSignedUrls = readFilesConfig();
        LOGGER.info("[{}] : Signed files Url [{}]", getConnectorId(), fileVsSignedUrls);
        Map<String, File> files = new HashMap<>();
        for (String fileName : fileVsSignedUrls.keySet()) {
            File file = storeFile(fileName, fileVsSignedUrls.get(fileName));
            files.put(fileName, file);
            //files.put("test.csv", new File("/home/ravi/Downloads/test.csv"));
            //files.put("kit_requests_clean.csv", new File("/home/ravi/Downloads/kit_requests_clean.csv"));
        }
        try {
            updateConnectorState(SYNC_STATUS, Status.STARTED, 0);
        } catch (Exception e) {
            LOGGER.error("Failed to update the sync state "+getConnectorId(), e);
        }
        LOGGER.info("[{}] : Local files Url [{}]", getConnectorId(), files);
        SyncDataResponse syncDataResponse = validateFileFormats(files);
        if (syncDataResponse != null) {
            return syncDataResponse;
        }
        List<RawEvent> vcEvents = new ArrayList<>();
        for (String fileName : files.keySet()) {
            File file = files.get(fileName);
            CSVEventSourceReader csvReader = null;
            try {
                csvReader = null;
                try {
                    csvReader = new CSVEventSourceReader(fileName, file, getConnectorId(), this, SYNC_DATA);
                    publishPreviewEvents(file, csvReader, vcEvents, PREVIEW_RECORDS, 1, 0,
                            false, true, true, false);
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

    private SyncDataResponse validateFileFormats(Map<String, File> files) {
        List<UnsupportedFormatException> unsupportedFormatExceptions = new ArrayList<>();
        for (String fileName : files.keySet()) {
            File file = files.get(fileName);
            CSVEventSourceReader csvReader = null;
            try {
                csvReader = null;
                try {
                    csvReader = new CSVEventSourceReader(fileName, file, getConnectorId(), this, SYNC_DATA);
                    csvReader.validateFileFormat();
                } finally {
                    if (csvReader != null) {
                        csvReader.close();
                    }
                }
            } catch (UnsupportedFormatException t) {
                unsupportedFormatExceptions.add(t);
            } catch (Throwable t) {
                throw new IllegalStateException("Failed to register preview events for discovery service ["+fileName+"]", t);
            }
        }
        if (!unsupportedFormatExceptions.isEmpty()) {
            return SyncDataResponse.newBuilder()
                    .setStatus(Status.ERROR)
                    .setResponse(StatusResponse.newBuilder().setMessage(Status.ERROR.name()).build())
                    .build();
        }
        return null;
    }

    private Future<Object> processFiles(Map<String, File> files) {
        BaseEventConnector connector = this;
        Future<Object> future = executorService.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    updateConnectorState(SYNC_STATUS, Status.IN_PROGRESS, 0);
                    int total_records = 0;
                    for (String fileName : files.keySet()) {
                        File file = files.get(fileName);
                        int records = totalRecords(file);
                        records = records - 1; //remove header
                        total_records = total_records + records;
                    }
                    Status status = Status.COMPLETE;
                    int validCount = 0;
                    for (String fileName : files.keySet()) {
                        File file = files.get(fileName);
                        CSVEventSourceReader csvReader = null;
                        try {
                            csvReader = new CSVEventSourceReader(fileName, file, getConnectorId(), connector, SYNC_DATA);
                            validCount = publishPreviewEvents(file, csvReader, Collections.emptyList(),
                                    Integer.MAX_VALUE, total_records, validCount,
                                    true, false, false, true);
                            if (csvReader.getStatus() != null && csvReader.getStatus().equals(ReaderStatus.FAILED)) {
                                status = Status.ERROR;
                            }
                        } finally {
                            if (csvReader != null) {
                                csvReader.close();
                            }
                        }
                    }
                    LOGGER.info("[{}] : Raw events total - Total Count[{}]", getConnectorId(), total_records);
                    updateConnectorState(SYNC_STATUS, status);
                } catch (Throwable t) {
                    LOGGER.error("Failed to submit records to preview store [{}]", getConnectorId(), t);
                    updateConnectorState(SYNC_STATUS, Status.ERROR);
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
        LOGGER.info("Starting doRead [{}] [{}]", config, state);
        initialize(config, catalog);
        initializeExecutors();
        LOGGER.info("Starting ingesting records [{}] [{}] [{}]", getConnectorId(), config, state);
        try {
            Status status = getConnectorStatus(READ_STATUS);
            if (status != null) {
                LOGGER.info("Already ingesting records [{}] [{}] [{}]", getConnectorId(), config, state);
                return null;
            }
            updateConnectorState(READ_STATUS, Status.STARTED, 0);
        } catch (Throwable e) {
            throw new IllegalStateException("Failed to update the sync state ["+getConnectorId()+"]");
        }
        EventSourceInfo eventSourceInfo = new EventSourceInfo(getConnectorId(), getEventSourceType());
        boolean doesMappingRulesExist = doesMappingRulesExists(getAuthInfo(), eventSourceInfo);

        Map<String, String> fileVsSignedUrls = readFilesConfig();
        LOGGER.info("[{}] : doRead Signed files Url [{}]", getConnectorId(), fileVsSignedUrls);
        Map<String, File> files = new HashMap<>();
        for (String fileName : fileVsSignedUrls.keySet()) {
            File file = storeFile(fileName, fileVsSignedUrls.get(fileName));
            files.put(fileName, file);
            //files.put(fileName, new File("/home/ravi/Downloads/test.csv"));
        }

        try {
            updateConnectorState(READ_STATUS, Status.IN_PROGRESS, 0);
        } catch (Throwable e) {
            throw new IllegalStateException("Failed to update the sync state ["+getConnectorId()+"]");
        }
        int batchSize = getBatchSize(config);
        Object[] objects = sortEvents(files, batchSize);
        long totalRecords = (long) objects[0];
        Map<Long, List<FileRecordOffset>> timestampToFileOffsetsMap = (Map<Long, List<FileRecordOffset>>) objects[1];

        boolean success = false;
        try {
            long processed = publishEvents(timestampToFileOffsetsMap, files, batchSize, totalRecords);
            updateConnectorState(READ_STATUS, Status.COMPLETE);
            if (processed > 0) {
                success = true;
            }
        } catch (IOException e) {
            LOGGER.error("Failed to process records ["+getConnectorId()+"]", e);
            try {
                updateConnectorState(READ_STATUS, Status.ERROR);
            } catch (Throwable ex) {
                LOGGER.error("Failed to read ["+getConnectorId()+"]", ex);
            }
            throw new IllegalStateException(e);
        } finally {
            if (success) {
                publishDummyEvents(getAuthInfo(), eventSourceInfo, getDummyMessagesInSecs(config));
            }
            stopEventConnector();
            LOGGER.info("doRead Done");
        }
        return null;
    }

    private void initializeExecutors() {
        if (executorService != null) {
            return;
        }
        runtimeConfig = connectorConfigManager.getRuntimeConfig(bicycleConfig.getAuthInfo(),
                                                                                bicycleConfig.getConnectorId());
        if (connectorConfigManager.isDefaultConfig(runtimeConfig)) {
            runtimeConfig = null;
        }
        int backlogExecutorPoolSize = runtimeConfig == null ?
                Integer.parseInt(getPropertyValue("BACKLOG_EXECUTOR_POOL_SIZE", "10")) :
                runtimeConfig.getConcurrencyConfig().getBacklogExecutorPoolSize();
        executorService = Executors.newFixedThreadPool(backlogExecutorPoolSize, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("csvconnector-lite-"+ threadcounter.incrementAndGet());
                return t;
            }
        });
    }

    private String getPropertyValue(String propertyName, String defaultValue) {
        String propValue = System.getenv(propertyName);
        if (StringUtils.isEmpty(propValue)) {
            propValue = System.getProperty(propertyName);
            if (StringUtils.isEmpty(propValue)) {
                propValue = defaultValue;
            }
        }
        return propValue;
    }

    private Object[] sortEvents(Map<String, File> files, int batchSize) {
        AtomicLong successCounter = new AtomicLong(0);
        AtomicLong failedCounter = new AtomicLong(0);
        Map<Long, List<FileRecordOffset>> timestampToFileOffsetsMap = new ConcurrentSkipListMap<>();
        Map<String, Future<Void>> futures = new HashMap<>();
        for (String fileName : files.keySet()) {
            File file = files.get(fileName);
            Future<Void> future = executorService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    readTimestampToFileOffset(timestampToFileOffsetsMap, fileName, file, batchSize,
                            successCounter, failedCounter);
                    return null;
                }
            });
            futures.put(fileName, future);
        }
        for (String fileName : futures.keySet()) {
            try {
                Future<Void> future = futures.get(fileName);
                future.get();
            } catch (Throwable t) {
                throw new IllegalStateException("Failed to sort events [" + fileName + "]", t);
            }
        }
        LOGGER.info("[{}] : Processed files by timestamp [{}] [{}] [{}]", getConnectorId(),
                                        successCounter.get(), failedCounter.get(), timestampToFileOffsetsMap.size());
        return new Object[]{successCounter.get(), timestampToFileOffsetsMap};
    }

    private long publishEvents(Map<Long, List<FileRecordOffset>> timestampToFileOffsetsMap,
                               Map<String, File> files, int batchSize, long totalRecords) {
        Map<Integer, Map<Long, List<FileRecordOffset>>> buckets = timestampToFileOffsetsMap.entrySet().stream()
                .collect(Collectors.groupingBy(entry -> Math.abs(entry.getKey().hashCode() % threads),
                        HashMap::new, Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                                      (oldValue, newValue) -> oldValue, TreeMap::new)));
        for (int index : buckets.keySet()) {
            Map<Long, List<FileRecordOffset>> bucket = buckets.get(index);
            LOGGER.info("Timestamp buckets size [{}] [{}]", getConnectorId(), buckets.size(), index, bucket.size());
        }
        AtomicLong successCounter = new AtomicLong(0);
        AtomicLong failedCounter = new AtomicLong(0);
        Map<Integer, Future<Void>> futures = new HashMap<>();
        for (int index : buckets.keySet()) {
            Map<Long, List<FileRecordOffset>> bucket = buckets.get(index);
            Future<Void> future = executorService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    processCSVFile(index, bucket, files, totalRecords, batchSize, successCounter, failedCounter);
                    return null;
                }
            });
            futures.put(index, future);
        }
        for (int i : futures.keySet()) {
            try {
                Future<Void> future = futures.get(i);
                future.get();
            } catch (Throwable t) {
                throw new IllegalStateException("Failed to publish events [" + i + "]", t);
            }
        }
        LOGGER.info("[{}] : Published events [{}] [{}] [{}]", getConnectorId(),
                successCounter.get(), failedCounter.get(), timestampToFileOffsetsMap.size());
        return successCounter.get();
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

    private int getBatchSize(JsonNode config) {
        return config.get("batchSize") != null ? config.get("batchSize").asInt() : BATCH_SIZE;
    }

    private int getDummyMessagesInSecs(JsonNode config) {
        return config.get("dummyMessageInterval") != null ? config.get("dummyMessageInterval").asInt() : 600;
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
