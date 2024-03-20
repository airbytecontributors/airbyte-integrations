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
import io.airbyte.integrations.bicycle.base.integration.csv.CSVEventSourceReader;
import io.airbyte.integrations.bicycle.base.integration.exception.UnsupportedFormatException;
import io.airbyte.integrations.bicycle.base.integration.job.*;
import io.airbyte.protocol.models.*;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.integration.common.Status;
import io.bicycle.integration.common.StatusResponse;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.integration.connector.SyncDataRequest;
import io.bicycle.integration.connector.SyncDataResponse;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static io.airbyte.integrations.bicycle.base.integration.BaseCSVEventConnector.APITYPE.READ;
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
        initializeExecutors();
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
        /*SyncDataResponse syncDataResponse = validateFileFormats(files);
        if (syncDataResponse != null) {
            LOGGER.info("validation of files failed [{}]", getConnectorId());
            return syncDataResponse;
        }*/
        List<RawEvent> vcEvents = new ArrayList<>();
        for (String fileName : files.keySet()) {
            File file = files.get(fileName);
            CSVEventSourceReader csvReader = null;
            try {
                csvReader = null;
                try {
                    csvReader = getCSVReader(fileName, file, getConnectorId(), this, SYNC_DATA);
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
                    csvReader = getCSVReader(fileName, file, getConnectorId(), this, SYNC_DATA);
                    csvReader.validateFileFormat();
                } finally {
                    if (csvReader != null) {
                        csvReader.close();
                    }
                }
            } catch (UnsupportedFormatException t) {
                LOGGER.info("validation of files failed [{}] [{}]", getConnectorId(), fileName, t);
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
                            csvReader = getCSVReader(fileName, file, getConnectorId(), connector, SYNC_DATA);
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
        boolean success = false;
        try {
            initialize(config, catalog);
            int threads = initializeExecutors();
            Status status = getConnectorStatus(READ_STATUS);
            if (status != null) {
                LOGGER.info("Already ingesting records [{}] [{}] [{}]", getConnectorId(), config, state);
            }
            updateConnectorState(READ_STATUS, Status.STARTED, 0);
            Map<String, File> files = new HashMap<>();
            Map<String, String> fileVsSignedUrls = readFilesConfig();
            LOGGER.info("[{}] : doRead Signed files Url [{}]", getConnectorId(), fileVsSignedUrls);
            for (String fileName : fileVsSignedUrls.keySet()) {
                File file = storeFile(fileName, fileVsSignedUrls.get(fileName));
                files.put(fileName, file);
            }


            updateConnectorState(READ_STATUS, Status.IN_PROGRESS, 0);
            int queueSize = getQueueSize(config);
            int requestSize = getRequestSize(config);
            long totalRecords = calculateTotalrecords(files, queueSize);
            try {
                long processed = publishEvents(files, queueSize, requestSize, threads, totalRecords);
                updateConnectorState(READ_STATUS, Status.COMPLETE);
                saveState("totalRecords", totalRecords);
                if (processed > 0) {
                    success = true;
                }
            } catch (IOException e) {
                LOGGER.error("Failed to process records ["+getConnectorId()+"]", e);
                updateConnectorState(READ_STATUS, Status.ERROR);
            }
        } catch (Throwable e) {
            throw new IllegalStateException("Failed to run read ["+getConnectorId()+"]", e);
        } finally {
            stopEventConnector();
            if (success) {
                LOGGER.info("doRead Success");
            } else {
                LOGGER.info("doRead Failed");
            }
        }
        return null;
    }

    private int initializeExecutors() {
        LOGGER.info("Initializing executors [{}]", executorService == null);
        runtimeConfig = connectorConfigManager.getRuntimeConfig(bicycleConfig.getAuthInfo(),
                                                                                bicycleConfig.getConnectorId());
        if (runtimeConfig != null && connectorConfigManager.isDefaultConfig(runtimeConfig)) {
            runtimeConfig = null;
        }
        boolean enableParallelism = runtimeConfig == null ?
                Boolean.parseBoolean(getPropertyValue("ENABLE_CONSUMER_CYCLE_PARALLELISM", "false")) :
                runtimeConfig.getConcurrencyConfig().getEnableConcurrency();
        int backlogExecutorPoolSize = runtimeConfig == null ?
                Integer.parseInt(getPropertyValue("BACKLOG_EXECUTOR_POOL_SIZE", "4")) :
                runtimeConfig.getConcurrencyConfig().getBacklogExecutorPoolSize();
        if (!enableParallelism) {
            backlogExecutorPoolSize = 1;
        }
        if (executorService == null) {
            LOGGER.info("Initializing executor pool size [{}] [{}] [{}]", getConnectorId(), backlogExecutorPoolSize,
                    enableParallelism);    executorService = Executors.newFixedThreadPool(backlogExecutorPoolSize, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setName("csvconnector-lite-"+ threadcounter.incrementAndGet());
                    return t;
                }
            });
        }
        return backlogExecutorPoolSize;
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

    private long publishEvents(Map<String, File> files, int queueSize, int requestSize, int threads, long totalRecords) {
        EventProcessMetrics metrics = new EventProcessMetrics(totalRecords);
        BicycleEventsProcessor eventsProcessor = new BicycleEventsProcessor(threads, threads * queueSize, metrics);
        for (int i = 0; i < threads; i++) {
            eventsProcessor.submit(getPublisherConsumerJob(metrics));
        }
        BicycleRulesProcessor rulesProcessor = new BicycleRulesProcessor(threads, threads * queueSize, metrics);
        for (int i = 0; i < threads; i++) {
            rulesProcessor.submit(getRulesConsumerJob(requestSize, eventsProcessor.getProducer()));
        }
        Map<String, Future<Boolean>> futures = new HashMap<>();
        for (String fileName : files.keySet()) {
            File file = files.get(fileName);
            Future<Boolean> future = rulesProcessor.submit(getRulesProducerJob(fileName, file, this));
            futures.put(fileName, future);
        }
        for (String fileName : futures.keySet()) {
            try {
                Future<Boolean> future = futures.get(fileName);
                future.get();
            } catch (Throwable t) {
                throw new IllegalStateException("Failed to publish events [" + fileName + "]", t);
            }
        }
        rulesProcessor.stop();
        eventsProcessor.stop();
        return metrics.getSuccess();
    }

    private ProducerJob<RawEvent> getRulesProducerJob(String fileName, File file, BaseEventConnector connector) {
        return new ProducerJob<RawEvent>() {
            public void process(Producer<RawEvent> producer) {
                CSVEventSourceReader reader = null;
                long count = 0;
                try {
                    reader = getCSVReader(fileName, file, getConnectorId(), connector, READ);
                    while (reader.hasNext()) {
                        RawEvent rawEvent = reader.next();
                        producer.produce(rawEvent);
                        count++;
                    }
                    LOGGER.info("[{}] : Finished Processing file [{}] [{}]", getConnectorId(), fileName, count);
                } catch (Exception e) {
                    LOGGER.info("[{}] : Failed to parse csv [{}]", getConnectorId(), fileName, e);
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (Exception e) {
                        }
                    }
                }
            }

            @Override
            public void finish() {
            }
        };
    }

    private ConsumerJob<RawEvent> getRulesConsumerJob(int requestSize, BicycleProducer<EventProcessorResult> producer) {
        return new ConsumerJob<RawEvent>() {

            private static AtomicLong counter = new AtomicLong(0);
            private static AtomicLong records = new AtomicLong(0);
            private static AtomicLong records1 = new AtomicLong(0);
            private AtomicLong bufferSize = new AtomicLong(0);
            private List<RawEvent> rawEvents = new ArrayList<>();

            private ReentrantLock lock = new ReentrantLock();

            public void process(RawEvent rawEvent) {
                try {
                    boolean acquired = lock.tryLock(60, TimeUnit.SECONDS);
                    if (bufferSize.get() > requestSize) {
                        publishEvents();
                    }
                    rawEvents.add(rawEvent);
                    /*LOGGER.info("[{}] Successfully Buffered records [{}] [{}] [{}] [{}]", getConnectorId(),
                            Thread.currentThread().getName(), counter.incrementAndGet(), rawEvents.size(), acquired);*/
                    JsonRawEvent jsonRawEvent = (JsonRawEvent) rawEvent;
                    byte[] bytes = jsonRawEvent.getJsonEvent().getJsonStr().getBytes();
                    bufferSize.addAndGet(bytes.length);
                } catch (InterruptedException e) {
                    LOGGER.error("Lock Interrupted", e);
                } finally {
                    lock.unlock();
                }
            }

            private void publishEvents() {
                if (rawEvents.size() == 0) {
                    return;
                }
                EventSourceInfo eventSourceInfo = new EventSourceInfo(getConnectorId(), getEventSourceType());
                EventProcessorResult eventProcessorResult = convertRawEventsToBicycleEvents(getAuthInfo(),
                        eventSourceInfo, rawEvents, getUserServiceMappingRules());
                producer.addToQueue(eventProcessorResult);
                LOGGER.info("[{}] Successfully Pushed Records [{}] [{}] [{}]", getConnectorId(),
                        Thread.currentThread().getName(), records.incrementAndGet(),
                        records1.addAndGet(eventProcessorResult.getUserServiceDefs().size()));
                rawEvents.clear();
                bufferSize.set(0);
            }

            public void finish() {
                try {
                    boolean acquired = lock.tryLock(60, TimeUnit.SECONDS);
                    LOGGER.info("[{}] Finishing RulesCosumerJob [{}] [{}] [{}]", getConnectorId(),
                            Thread.currentThread().getName(), rawEvents.size(), acquired);
                    publishEvents();
                } catch (InterruptedException e) {
                    LOGGER.error("Lock Interrupted", e);
                } finally {
                    lock.unlock();
                }

            }
        };
    }

    private ConsumerJob<EventProcessorResult> getPublisherConsumerJob(EventProcessMetrics metrics) {
        return new ConsumerJob<EventProcessorResult>() {

            private static AtomicLong records = new AtomicLong(0);
            private static AtomicLong counter = new AtomicLong(0);

            public void process(EventProcessorResult eventProcessorResult) {
                long startTimeInMillis = System.currentTimeMillis();
                int size = eventProcessorResult.getUserServiceDefs().size();
                records.incrementAndGet();
                counter.addAndGet(size);
                if (size > 0) {
                    String connectorId = getConnectorId();
                    boolean publishEvents = publishEvents(getAuthInfo(), eventSourceInfo, eventProcessorResult);
                    if (publishEvents) {
                        metrics.success(size);
                    } else {
                        metrics.failed(size);
                    }
                    LOGGER.info("[{}] Success published records [{}] [{}] batch [{}] counter [{}] time [{}]", connectorId, metrics.getSuccess(),
                            metrics.getFailed(), records.get(), counter.get(), (System.currentTimeMillis() - startTimeInMillis));
                    try {
                        updateConnectorState(READ_STATUS, Status.IN_PROGRESS, ((double) metrics.getSuccess() / (double) metrics.getTotalRecords()));
                    } catch (Exception e) {
                        LOGGER.error("[{}] Failed to update state [{}] [{}]", connectorId, metrics.getSuccess(),
                                metrics.getFailed(), e);
                    }
                }
            }

            public void finish() {
            }
        };
    }

    private long calculateTotalrecords(Map<String, File> files, int batchSize) {
        long start = System.currentTimeMillis();
        AtomicLong successCounter = new AtomicLong(0);
        AtomicLong failedCounter = new AtomicLong(0);
        Map<String, Future<Void>> futures = new HashMap<>();
        for (String fileName : files.keySet()) {
            File file = files.get(fileName);
            Future<Void> future = executorService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    readFileRecords(fileName, file, successCounter);
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
                throw new IllegalStateException("Failed to read events [" + fileName + "]", t);
            }
        }
        LOGGER.info("[{}] : Processed files by timestamp [{}] [{}] [{}]", getConnectorId(),
                successCounter.get(), failedCounter.get(), (System.currentTimeMillis() - start));
        return successCounter.get();
    }

    public List<RawEvent> convertRecordsToRawEventsInternal(List<?> records) {
        return null;
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

    private int getRequestSize(JsonNode config) {
        return config.get("requestSize") != null ? config.get("requestSize").asInt() : 524288;
    }

    private int getQueueSize(JsonNode config) {
        return config.get("queueSize") != null ? config.get("queueSize").asInt() : 100;
    }
}
