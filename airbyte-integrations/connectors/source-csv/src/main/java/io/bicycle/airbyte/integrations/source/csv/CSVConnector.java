package io.bicycle.airbyte.integrations.source.csv;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Charsets;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.CommonUtils;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.*;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.integration.common.bicycleconfig.BicycleConfig;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.server.event.mapping.UserServiceMappingRule;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * @author <a href="mailto:ravi.noothi@agilitix.ai">Ravi Kiran Noothi</a>
 * @since 14/11/22
 */

public class CSVConnector extends BaseEventConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVConnector.class);

    private static final String SEPARATOR_CHAR = ",";

    private static final String BACKFILL_COMPLETE = "backfill";
    private static final String LAST_RUNTIME_IN_MILLIS = "LAST_RUNTIME_IN_MILLIS";
    private static final String BACKFILL_RUNTIME_IN_MILLIS = "BACKFILL_RUNTIME_IN_MILLIS";

    private static final String PREVIOUS_BUCKET_NUMBER = "PREVIOUS_BUCKET_NUMBER";
    private static final String PREVIOUS_BUCKET_START_TIME_IN_MILLIS = "PREVIOUS_BUCKET_START_TIME_IN_MILLIS";

    private static final String BACKFILL_PREVIOUS_BUCKET_NUMBER = "BACKFILL_PREVIOUS_BUCKET_NUMBER";
    private static final String BACKFILL_PREVIOUS_BUCKET_START_TIME_IN_MILLIS = "BACKFILL_PREVIOUS_BUCKET_START_TIME_IN_MILLIS";
    private volatile boolean shutdown = false;

    private static final long WEEK_TIME_IN_MILLIS = (7 * 86400 * 1000);
    private static final int BATCH_SIZE = 1000;
    private int CONNECT_TIMEOUT_IN_MILLIS = 60000;
    private int READ_TIMEOUT_IN_MILLIS = 60000;

    private long epochOffsetTimeInMillis = 0;
    private long csvDataStartTimeInMillis = 0;
    private long csvDataDurationInMillis = 0;

    private File file;
    private String[] headers;

    private ObjectMapper mapper = new ObjectMapper();

    private String csvUrl;
    private String datasetName;
    private String format;
    private String timestampHeaderField;
    private String timestampformat;
    private String timeZone;
    private String backfillStartTimestamp;
    private String backfillEndTimestamp;

    private long periodicityInMillis = 60000;
    private long backfillSleepTimeInMillis = 2000;

    private boolean publishEventsEnabled = true;

    private List<CSVRecord> records = new ArrayList<>();

    public CSVConnector(SystemAuthenticator systemAuthenticator,
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


    private void storeToFile(JsonNode config, File file) throws IOException {
        final JsonNode provider = config.get("provider");
        csvUrl = getCsvUrl(config);
        if (provider.get("storage").asText().equals("GCS")) {
            String serviceAccount = provider.get("service_account_json").asText();
            final ServiceAccountCredentials credentials = ServiceAccountCredentials
                    .fromStream(new ByteArrayInputStream(serviceAccount.getBytes(Charsets.UTF_8)));
            Storage storage = StorageOptions.newBuilder().setCredentials(credentials)
                    .build().getService();
            Blob blob = storage.get(BlobId.fromGsUtilUri(csvUrl));
            blob.downloadTo(file.toPath());
        } else {
            FileUtils.copyURLToFile(new URL(csvUrl), file, CONNECT_TIMEOUT_IN_MILLIS, READ_TIMEOUT_IN_MILLIS);
        }
    }

    public AutoCloseableIterator<AirbyteMessage> preview(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) throws InterruptedException, ExecutionException {
        LOGGER.info("Preview CSV data");
        try {
            String csvUrl = getCsvUrl(config);
            this.timestampHeaderField = config.get("timeHeader").asText();
            this.timestampformat = config.get("timeFormat").asText();
            this.timeZone = config.get("timeZone") != null ? config.get("timeZone").asText() : "UTC";
            String datasetName = getDatasetName(config);
            File file = File.createTempFile(UUID.randomUUID().toString(),".csv");
            storeToFile(config, file);
            CSVParser parsed = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new FileReader(file, Charset.defaultCharset()));
            Iterator<org.apache.commons.csv.CSVRecord> iterator = parsed.iterator();
            return AutoCloseableIterators.fromIterator(new AbstractIterator<>() {
                @Override
                protected AirbyteMessage computeNext() {
                    if (iterator.hasNext()) {
                        org.apache.commons.csv.CSVRecord record = iterator.next();
                        ObjectNode objectNode = mapper.createObjectNode();
                        for (Map.Entry<String, String> entry : record.toMap().entrySet()) {
                            if (entry.getKey().equals(timestampHeaderField)) {
                                getUTCTimesupplier().apply(entry.getValue());
                            }
                            objectNode.put(entry.getKey(), entry.getValue());
                        }
                        return new AirbyteMessage()
                                .withType(AirbyteMessage.Type.RECORD)
                                .withRecord(new AirbyteRecordMessage()
                                        .withStream(datasetName)
                                        .withEmittedAt(Instant.now().toEpochMilli())
                                        .withData(objectNode));
                    }
                    return endOfData();
                }
            });
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    public AirbyteConnectionStatus check(JsonNode config) throws Exception {
        LOGGER.info("Check the status");
        String csvUrl = getCsvUrl(config);
        if (csvUrl  == null) {
            return new AirbyteConnectionStatus()
                    .withStatus(AirbyteConnectionStatus.Status.FAILED)
                    .withMessage("URL is not provided.");
        } else {
            CSVParser parsed = null;
            try {
                File file = File.createTempFile(UUID.randomUUID().toString(),".csv");
                storeToFile(config, file);
                parsed = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new FileReader(file, Charset.defaultCharset()));
                parsed.iterator().hasNext();
                return new AirbyteConnectionStatus()
                        .withStatus(AirbyteConnectionStatus.Status.SUCCEEDED)
                        .withMessage("Success");
            } catch (Exception e) {
                LOGGER.error("Processing the csv file failed [{}] ", csvUrl, e);
                return new AirbyteConnectionStatus()
                        .withStatus(AirbyteConnectionStatus.Status.FAILED)
                        .withMessage("Processing the CSV file failed");
            } finally {
                if (parsed != null) {
                    parsed.close();
                }
            }

        }
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

    public AutoCloseableIterator<AirbyteMessage> doRead(JsonNode config, ConfiguredAirbyteCatalog catalog,
                                                      JsonNode state) throws Exception{
        LOGGER.info("Inside do read command");
        int runCount = 1;
        if (shutdown) {
            return null;
        }
        state = getStateAsJsonNode(getAuthInfo(), getConnectorId());
        if (state == null) {
            LOGGER.info("Initialized State is null [{}] [{}]", getTenantId(), getConnectorId());
        } else if(state.isEmpty()) {
            LOGGER.info("Initialized State is Empty [{}] [{}]", getTenantId(), getConnectorId());
        } else {
            LOGGER.info("Initialized State [{}]  [{}] [{}]", state, getTenantId(), getConnectorId());
        }

        try {
            LOGGER.info("Starting Read v5 [{}]  [{}]  [{}]", getTenantId(), getConnectorId(), runCount);
            this.csvUrl = getCsvUrl(config);
            if (csvUrl == null) {
                throw new IllegalStateException("No csv url");
            }
            this.datasetName = getDatasetName(config);
            this.format = config.get("format").asText();
            this.timestampHeaderField = config.get("timeHeader").asText();
            this.timestampformat = config.get("timeFormat").asText();
            this.timeZone = config.get("timeZone") != null ? config.get("timeZone").asText() : "UTC";
            this.backfillStartTimestamp
                    = config.get("backfillStartDateTime") != null ? config.get("backfillStartDateTime").asText() : null;
            this.backfillEndTimestamp
                    = config.get("backfillEndDateTime") != null ? config.get("backfillEndDateTime").asText() : null;
            this.publishEventsEnabled
                    = config.get("publishEventsEnabled") != null ? config.get("publishEventsEnabled").asBoolean() : true;

            //this.timeZone = config.get("timeZone").asText();
            this.periodicityInMillis
                    = config.get("periodicity") != null ? config.get("periodicity").asInt() * 1000 : periodicityInMillis;
            boolean backfill = config.get("backfill") != null ? config.get("backfill").asBoolean() : false;
            boolean replay = config.get("replay") != null ? config.get("replay").asBoolean() : true;

            this.backfillSleepTimeInMillis
                    = additionalProperties != null && additionalProperties.get("backfillSleepTimeInMillis") != null
                    ? Long.parseLong(additionalProperties.get("backfillSleepTimeInMillis").toString()) : 2000;

            Map<Long, Map<Long, List<Long>>> bucketVsRecords
                    = readFile(config, csvUrl, timestampHeaderField, getUTCTimesupplier());
            LOGGER.info("Read File Summary [{}] [{}] size [{}] [{}]", getTenantId(), getConnectorId(),
                    bucketVsRecords.size(), bucketVsRecords.size() > 20 ?  "" : bucketVsRecords);

            if (csvDataDurationInMillis == 0) {
                throw new IllegalStateException("Incorrect data duration in the csv file["+getTenantId()+"] ["+csvUrl+"]");
            }

            if (csvDataDurationInMillis < periodicityInMillis) {
                periodicityInMillis = csvDataDurationInMillis;
                LOGGER.info("Data in csv is less than periodicty given [{}] [{}]. " +
                        "Default to csvDataDurationInMillis to periodicityInMillis", getTenantId(), csvUrl);
            }

            do {
                try {
                    long lastPublishedTimeInMillis = backfill(backfill, bucketVsRecords);
                    replay(replay, lastPublishedTimeInMillis, bucketVsRecords);
                } catch(Throwable e) {
                    LOGGER.error("Exception while running ["+getTenantId()+"] : ["+getConnectorId()+"]" , e);
                }
            } while (!shutdown);

        } catch(Throwable e) {
            LOGGER.error("Exception in the job ["+getTenantId()+"] : ["+getConnectorId()+"]" , e);
        } finally {
            LOGGER.info("Completed Read v3 [{}] [{}] [{}]", getTenantId(), getConnectorId(), runCount);
        }
        return  null;
    }

    private void replay(boolean replay, long lastPublishedTimeInMillis,
                        Map<Long, Map<Long, List<Long>>> bucketVsRecords) throws Exception {
        long previousBucketNumber = -1;
        long previousBucketStartTimeMillis = -1;

        long lastRunTimeInMillis = getStateAsLong(LAST_RUNTIME_IN_MILLIS);
        LOGGER.info("Replay [{}] [{}] [{}] [{}]", getTenantId(), getConnectorId(),
                lastRunTimeInMillis, lastPublishedTimeInMillis);
        lastRunTimeInMillis = lastRunTimeInMillis == -1  ? lastPublishedTimeInMillis :
                lastRunTimeInMillis > lastPublishedTimeInMillis ? lastRunTimeInMillis : lastPublishedTimeInMillis;
        while (replay && !shutdown) {
            long currentTimeInMillis = -1;
            if (lastRunTimeInMillis == -1) {
                currentTimeInMillis = System.currentTimeMillis();
            } else {
                if (lastRunTimeInMillis + periodicityInMillis > System.currentTimeMillis()) {
                    currentTimeInMillis = System.currentTimeMillis();
                } else {
                    currentTimeInMillis = lastRunTimeInMillis + periodicityInMillis;
                }
            }

            if (previousBucketNumber == -1) {
                previousBucketNumber = getStateAsLong(PREVIOUS_BUCKET_NUMBER);
            }
            if (previousBucketStartTimeMillis == -1) {
                previousBucketStartTimeMillis = getStateAsLong(PREVIOUS_BUCKET_START_TIME_IN_MILLIS);
            }

            long startTimeInMillis = getWindowStartTimeInMillis(currentTimeInMillis);
            long bucketNumber = getBucketNumber(currentTimeInMillis);
            process(bucketVsRecords, previousBucketNumber, previousBucketStartTimeMillis,
                        bucketNumber, startTimeInMillis,
                        currentTimeInMillis, 2000L);
            if (previousBucketNumber != bucketNumber) {
                previousBucketNumber = bucketNumber;
                previousBucketStartTimeMillis = startTimeInMillis;
                saveState(PREVIOUS_BUCKET_NUMBER, previousBucketNumber);
                saveState(PREVIOUS_BUCKET_START_TIME_IN_MILLIS, previousBucketStartTimeMillis);
            }
            saveState(LAST_RUNTIME_IN_MILLIS, currentTimeInMillis);
            lastRunTimeInMillis = currentTimeInMillis;
        }
    }

    private long backfill(boolean backfill, Map<Long, Map<Long, List<Long>>> bucketVsRecords) throws Exception {
        boolean stateAsBoolean = getStateAsBoolean(BACKFILL_COMPLETE);
        if (backfill && !stateAsBoolean) {
            long previousBackfillRuntimeInMillis = getStateAsLong(BACKFILL_RUNTIME_IN_MILLIS);
            long backfillTimeInMillisStart = getUTCTimesupplier().apply(backfillStartTimestamp);
            long backfillTimeInMillisEnd = backfillEndTimestamp != null ?
                    getUTCTimesupplier().apply(backfillEndTimestamp) : -1;
            LOGGER.info("Enabled backfill [{}] [{}] [{}] [{}] [{}]", getTenantId(), getConnectorId(),
                    previousBackfillRuntimeInMillis, backfillTimeInMillisStart, backfillTimeInMillisEnd);

            long timeInMillis;
            if (previousBackfillRuntimeInMillis != -1
                    && previousBackfillRuntimeInMillis >= backfillTimeInMillisStart
                    && previousBackfillRuntimeInMillis < backfillTimeInMillisEnd) {
                timeInMillis = previousBackfillRuntimeInMillis;
            } else {
                timeInMillis = backfillTimeInMillisStart;
            }
            LOGGER.info("Backfill Start [{}] [{}] [{}] [{}] [{}]", getTenantId(), getConnectorId(),
                    timeInMillis, backfillTimeInMillisStart, backfillTimeInMillisEnd);
            long previousBucketNumber = -1;
            long previousBucketStartTimeMillis = -1;
            long lastpublishedTimeInMillis = -1;
            while (!shutdown &&
                    ((backfillTimeInMillisEnd == -1 && timeInMillis < (System.currentTimeMillis() - periodicityInMillis))
                    || (timeInMillis <= backfillTimeInMillisEnd))) {

                if (previousBucketNumber == -1) {
                    previousBucketNumber = getStateAsLong(BACKFILL_PREVIOUS_BUCKET_NUMBER);
                }
                if (previousBucketStartTimeMillis == -1) {
                    previousBucketStartTimeMillis = getStateAsLong(BACKFILL_PREVIOUS_BUCKET_START_TIME_IN_MILLIS);
                }

                long startTimeInMillis = getWindowStartTimeInMillis(timeInMillis);
                long bucketNumber = getBucketNumber(timeInMillis);
                process(bucketVsRecords, previousBucketNumber, previousBucketStartTimeMillis,
                            bucketNumber, startTimeInMillis,
                            timeInMillis, backfillSleepTimeInMillis);

                if (previousBucketNumber != bucketNumber) {
                    previousBucketNumber = bucketNumber;
                    previousBucketStartTimeMillis = startTimeInMillis;
                    saveState(BACKFILL_PREVIOUS_BUCKET_NUMBER, previousBucketNumber);
                    saveState(BACKFILL_PREVIOUS_BUCKET_START_TIME_IN_MILLIS, previousBucketStartTimeMillis);
                }
                lastpublishedTimeInMillis = timeInMillis;
                timeInMillis = timeInMillis + periodicityInMillis;
                saveState(BACKFILL_RUNTIME_IN_MILLIS, timeInMillis);
            }
            LOGGER.info("Backfill Complete [{}] [{}] [{}]", getTenantId(), getConnectorId(), timeInMillis);
            if (!shutdown)
                saveState(BACKFILL_COMPLETE, true);
            return lastpublishedTimeInMillis;
        } else {
            LOGGER.info("Disabled backfill [{}] [{}] [{}] [{}]", getTenantId(), getConnectorId(), shutdown,
                    stateAsBoolean);
            return  -1;
        }
    }

    private boolean process(Map<Long, Map<Long, List<Long>>> bucketVsRecords,
                         long previousBucketNumber, long previousBucketStartTimeMillis,
                         long currentBucketNumber, long currentBucketStartTimeMillis,
                         long currentTimeInMillis, long sleepTimeInMillis) throws Exception {
        String eventSourceType = getEventSourceType();
        String connectorId = getConnectorId();
        LOGGER.info("Processing Records [{}] [{}] [{}] [{}] [{}] [{}]", getTenantId(), getConnectorId(),
                new Date(currentBucketStartTimeMillis), new Date(currentTimeInMillis), previousBucketNumber, currentBucketNumber);
        if (currentBucketNumber != previousBucketNumber) {
            Map<Long, List<Long>> publishRecords = bucketVsRecords.get(previousBucketNumber);
            if (publishRecords != null) {
                if (previousBucketStartTimeMillis != -1) {
                    LOGGER.info("Publishing Events [{}] [{}] [{}] [{}]  [{}] [{}] previous bucket[{}] current bucket[{}]", getTenantId(),
                            getConnectorId(), new Date(previousBucketStartTimeMillis), new Date(currentBucketStartTimeMillis),
                            new Date(currentTimeInMillis), publishRecords.size(), previousBucketNumber, currentBucketNumber);
                    publish(getAuthInfo(), connectorId, eventSourceType, timestampHeaderField,
                            getTimeZoneSupplier(previousBucketStartTimeMillis, previousBucketNumber),
                            publishRecords);
                } else {
                    LOGGER.info("Ignoring Publishing  Events [{}] [{}] [{}] previous bucket[{}] current bucket[{}]",
                            getTenantId(), getConnectorId(), publishRecords.size(), previousBucketNumber, currentBucketNumber);
                }

            }
            return true;
        } else {
            try {
                Thread.sleep(sleepTimeInMillis);
            } catch (Exception e){
            }
        }
        return false;
    }

    @NotNull
    private Function<Object, String> getTimeZoneSupplier(long startTimeInMiilis, long finalPreviousBucketNumber) {
        return (timestamp) -> {
            long relativeTimeInMillis
                    = (startTimeInMiilis) //new window startTime
                    + ((Long)timestamp - csvDataStartTimeInMillis); //Duration since the window start in original csv
            ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(relativeTimeInMillis), ZoneId.of(timeZone));
            //Date date = new Date(relativeTimeInMillis);
            //DateFormat dateFormat = new SimpleDateFormat(timestampformat);
            //return dateFormat.format(date);
            LOGGER.info("Event Publish Time [{}] [{}] [{}] [{}] [{}]", getTenantId(), getConnectorId(),
                    zdt, ZonedDateTime.ofInstant(Instant.ofEpochMilli((long)timestamp), ZoneId.of(timeZone)),
                    finalPreviousBucketNumber);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timestampformat);
            return zdt.format(formatter);
        };
    }

    private Function<String, Long> getUTCTimesupplier() {
        return (value) -> {
            DateTimeFormatter formatter;
            LocalDateTime localDateTime;
            try {
                formatter = DateTimeFormatter.ofPattern(timestampformat);
                localDateTime = LocalDateTime.parse(value, formatter);
            } catch (Throwable t) {
                try {
                    formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                    localDateTime = LocalDateTime.parse(value, formatter);
                } catch (Throwable t1) {
                    try {
                        formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");
                        localDateTime = LocalDateTime.parse(value, formatter);
                    } catch (Throwable t3) {
                        localDateTime = LocalDateTime.parse(value);
                        t3.printStackTrace();
                    }
                }
            }
            ZoneId z = ZoneId.of(timeZone);
            ZonedDateTime zdt = localDateTime.atZone(z) ;
            return zdt.toInstant().getEpochSecond() * 1000;
            //Timestamp timestamp = Timestamp.valueOf(localDateTime);
            //return timestamp.getTime();
        };
    }

    public Map<Long, Map<Long, List<Long>>> readFile(JsonNode config, String csvUrl, String timestampHeaderField,
                                               Function<String, Long> timeSupplier)
                                               throws Exception {
        LOGGER.info("Started Downloading file [{}]", getTenantId(), getConnectorId());
        file = createFile(config, csvUrl);
        Map<Long, Map<Long, List<Long>>> timebucketVsRecords = new LinkedHashMap<>();
        long start = System.currentTimeMillis();
        Map<Long, List<Long>> timeInMillisEpochVsRecordNumber
                = calculateCSVDurationAndRecordsOffset(additionalProperties, file, timeSupplier);
        for(Map.Entry<Long, List<Long>> entry : timeInMillisEpochVsRecordNumber.entrySet()) {
            Long timestampInMillis = entry.getKey();
            if (csvDataStartTimeInMillis != 0 && timestampInMillis > 0) {
                long bucketNumber = getBucketNumber(timestampInMillis);
                timebucketVsRecords.computeIfAbsent(bucketNumber,
                        (timeInMillis) -> new LinkedHashMap<>()).put(timestampInMillis, entry.getValue());
            }
        }
        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        LOGGER.info("Took " + timeTaken + " ms for uploading contents of file [{}] [{}]", getTenantId(), csvUrl);
        return timebucketVsRecords;
    }

    private File createFile(JsonNode config, String csvUrl) throws IOException {
        file = File.createTempFile(UUID.randomUUID().toString(),".csv");
        file.deleteOnExit();
        LOGGER.info("Coping the csv file to location [{}] [{}]", getTenantId(), file);
        long startTimeInMiilis = System.currentTimeMillis();
        storeToFile(config, file);
        LOGGER.info("Copied the csv file to location [{}] [{}] [{}]", getTenantId(), file,
                (System.currentTimeMillis() - startTimeInMiilis));
        return file;
    }

    private long  getBucketNumber(long timestampInMillis) {
        long windowStartTimeInMillis = getWindowStartTimeInMillis(timestampInMillis);
        long durationInMiilis = timestampInMillis - windowStartTimeInMillis;
        long bucketNumber = (durationInMiilis/1000) / (periodicityInMillis/1000);
        return bucketNumber;
    }

    private long getWindowStartTimeInMillis(long timeInMillis) {
        long durationInMillis = timeInMillis % csvDataDurationInMillis;
        long startTimeInMillis = timeInMillis - durationInMillis + epochOffsetTimeInMillis;
        startTimeInMillis = startTimeInMillis <= timeInMillis ?
                            startTimeInMillis : startTimeInMillis  - csvDataDurationInMillis;
        return startTimeInMillis;
    }

    public void publish(AuthInfo authInfo, String connectorId, String eventSourceType,
                        String timestampHeaderField, Function<Object, String> timeSupplier,
                        Map<Long, List<Long>> timeVsRecordNumber) throws Exception {
        eventSourceInfo = new EventSourceInfo(connectorId, eventSourceType);
        List<RawEvent> rawEvents = convertRecordsToRawEvents(timestampHeaderField, timeSupplier,
                 timeVsRecordNumber);
        if (publishEventsEnabled) {
            List<UserServiceMappingRule> rules = getUserServiceMappingRules(authInfo, eventSourceInfo);
            EventProcessorResult eventProcessorResult = convertRawEventsToBicycleEvents(authInfo, eventSourceInfo,
                    rawEvents, rules);
            publishEvents(authInfo, eventSourceInfo, eventProcessorResult);
        }
    }

    public List<RawEvent> convertRecordsToRawEvents(String timestampHeaderField,
                                                    Function<Object, String> timeSupplier,
                                                    Map<Long, List<Long>> timeVsRecordNumber) throws Exception {
        Set<Map.Entry<Long, List<Long>>> entries = timeVsRecordNumber.entrySet();
        List<RawEvent> rawEvents = new ArrayList<>();
        RandomAccessFile accessFile = new RandomAccessFile(file, "r");
        try {
            for(Map.Entry<Long, List<Long>> entry : entries) {
                Object timestamp =  entry.getKey();
                List<Long> recordsOffset = entry.getValue();
                for (long recordOffset : recordsOffset) {
                    CSVRecord record = getCsvRecord(recordOffset, accessFile);
                    if (record == null) {
                        continue;
                    }
                    ObjectNode node = mapper.createObjectNode();
                    for (Map.Entry<String, String> e : record.toMap().entrySet()) {
                        String key = e.getKey();
                        String value = e.getValue();
                        if (key.equals(timestampHeaderField)) {
                            node.put("original_"+key, value);
                            value = timeSupplier.apply(timestamp);

                        }
                        node.put(key, value);

                    }
                    JsonRawEvent jsonRawEvent = createJsonRawEvent(node);
                    rawEvents.add(jsonRawEvent);
                }
            }
            if (rawEvents.size() == 0) {
                return null;
            }
            return rawEvents;
        } finally {
            if (accessFile != null) {
                accessFile.close();
            }
        }
    }

    private CSVRecord getCsvRecord(long recordOffset, RandomAccessFile accessFile) throws Exception {
        accessFile.seek(recordOffset);
        String row = accessFile.readLine();
        CSVRecord csvRecord = getCsvRecord(recordOffset, row);
        return csvRecord;
    }

    public List<RawEvent> convertRecordsToRawEvents(List<?> records) {
        return null;
    }

    private String getCsvUrl(JsonNode config) {
        return config.get("url") != null ? config.get("url").asText() : null;
    }

    private String getDatasetName(JsonNode config) {
        return config.get("datasetName") != null ? config.get("datasetName").asText() : null;
    }

    private Map<Long, List<Long>> calculateCSVDurationAndRecordsOffset(Map<String, Object> additionalProperties,
                                                                       File file, Function<String, Long> timeSupplier)
            throws Exception {
        RandomAccessFile accessFile = new RandomAccessFile(file, "r");
        try {
            String headersLine = accessFile.readLine();
            if (headersLine != null && !headersLine.isEmpty()) {
                headers = sanitize(headersLine.trim());
                if (headers == null || headers.length == 0) {
                    throw new IllegalStateException("No headers available for csv");
                }
                LOGGER.info("CSV File Headers {}", headers);
            }
            Map<Long, List<Long>> timestampVsRecordsOffset = new TreeMap<>(new Comparator<Long>() {
                public int compare(Long aLong, Long t1) {
                    return aLong < t1 ? -1 : aLong > t1 ? 1 : 0;
                }
            });
            long csvStartTimeInMillisInEpoch = Long.MAX_VALUE;
            long csvEndTimeInMillisInEpoch = Long.MIN_VALUE;
            int recordNumber = 0;
            int numberOfEmptyLines = 0;
            do {
                long offset = accessFile.getFilePointer();
                String row = accessFile.readLine();
                if (row != null && !row.isEmpty()) {
                    numberOfEmptyLines = 0;
                    recordNumber++;
                    CSVRecord record = getCsvRecord(offset, row);
                    if (record == null) {
                        continue;
                    }
                    long timestampInMillisInEpoch = 0;
                    for (Map.Entry<String, String> entry : record.toMap().entrySet()) {
                        String key = entry.getKey();
                        String value = entry.getValue();
                        // fix event date
                        if (key.equals(timestampHeaderField)) {
                            timestampInMillisInEpoch = timeSupplier.apply(value);
                            if (timestampInMillisInEpoch < csvStartTimeInMillisInEpoch) {
                                csvStartTimeInMillisInEpoch = timestampInMillisInEpoch;
                            }
                            if (timestampInMillisInEpoch > csvEndTimeInMillisInEpoch ) {
                                csvEndTimeInMillisInEpoch = timestampInMillisInEpoch;
                            }
                        }
                    }
                    if (timestampInMillisInEpoch != 0) {
                        timestampVsRecordsOffset.computeIfAbsent(timestampInMillisInEpoch,
                                (recordOffset) -> new ArrayList<>()).add(offset);
                    } else {
                        LOGGER.error("Missing timestamp fields for the record [{}] [{}] [{}]", getTenantId(), getConnectorId(), record.toMap());
                    }
                    if (recordNumber % 10000 == 0) {
                        LOGGER.info("Processing the records [{}] [{}] [{}]", getTenantId(), getConnectorId(), recordNumber);
                    }
                } else if (numberOfEmptyLines < 100) {
                    LOGGER.info("Empty Line");
                    numberOfEmptyLines++;
                    continue;
                } else {
                    break;
                }
            } while (true);

            if (csvStartTimeInMillisInEpoch == Long.MAX_VALUE || csvEndTimeInMillisInEpoch == Long.MIN_VALUE) {
                throw new IllegalStateException("Could parse csv the start and end times");
            }
            LOGGER.info("Total Records [{}] [{}] [{}]", getTenantId(), getConnectorId(), recordNumber);
            if (csvStartTimeInMillisInEpoch != 0  && csvEndTimeInMillisInEpoch != 0) {
                long remainder = (csvEndTimeInMillisInEpoch - csvStartTimeInMillisInEpoch) % periodicityInMillis;
                if (remainder != 0) {
                    long count = (csvEndTimeInMillisInEpoch - csvStartTimeInMillisInEpoch) / periodicityInMillis;
                    csvDataDurationInMillis = (count + 1) * periodicityInMillis;
                } else {
                    csvDataDurationInMillis = (csvEndTimeInMillisInEpoch - csvStartTimeInMillisInEpoch);
                }
                csvDataStartTimeInMillis = csvStartTimeInMillisInEpoch;
                epochOffsetTimeInMillis = (csvDataStartTimeInMillis % csvDataDurationInMillis);
                double numOfWeeks = csvDataDurationInMillis / Double.valueOf(WEEK_TIME_IN_MILLIS);
                if (numOfWeeks > 1) {
                    if (Math.ceil(numOfWeeks) != numOfWeeks) {
                        Map<Long, List<Long>> map = new TreeMap<>(new Comparator<Long>() {
                            public int compare(Long aLong, Long t1) {
                                return aLong < t1 ? -1 : aLong > t1 ? 1 : 0;
                            }
                        });
                        long durationInMillis = (long) (Math.ceil(numOfWeeks) * WEEK_TIME_IN_MILLIS);
                        long endTimeInMillis = csvDataStartTimeInMillis + durationInMillis - ((long) Math.floor(numOfWeeks) * WEEK_TIME_IN_MILLIS);
                        long startTimeInMillis = (csvEndTimeInMillisInEpoch - ((long) Math.floor(numOfWeeks) * WEEK_TIME_IN_MILLIS));
                        for (long timestamp : timestampVsRecordsOffset.keySet()) {
                            if (timestamp > startTimeInMillis && timestamp <= endTimeInMillis) {
                                map.put((timestamp+((long) Math.floor(numOfWeeks) * WEEK_TIME_IN_MILLIS)), timestampVsRecordsOffset.get(timestamp));
                            }
                            if (timestamp > endTimeInMillis) {
                                break;
                            }
                        }
                        timestampVsRecordsOffset.putAll(map);
                        csvDataDurationInMillis = durationInMillis;
                        epochOffsetTimeInMillis = (csvDataStartTimeInMillis % csvDataDurationInMillis);
                    }
                }
                LOGGER.info("CSV startTimeInMillis [{}] [{}] [{}] [{}] [{}]", getTenantId(),
                        getConnectorId(), csvStartTimeInMillisInEpoch, csvEndTimeInMillisInEpoch,
                        csvDataDurationInMillis);
            }
            return timestampVsRecordsOffset;
        } finally {
            if (accessFile != null) {
                accessFile.close();
            }
        }
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
