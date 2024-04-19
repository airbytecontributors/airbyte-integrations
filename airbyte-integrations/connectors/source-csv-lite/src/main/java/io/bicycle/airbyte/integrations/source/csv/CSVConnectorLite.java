package io.bicycle.airbyte.integrations.source.csv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.integrations.bicycle.base.integration.*;
import io.airbyte.integrations.bicycle.base.integration.reader.EventSourceReader;
import io.airbyte.integrations.bicycle.base.integration.exception.UnsupportedFormatException;
import io.airbyte.integrations.bicycle.base.integration.job.config.ConsumerConfig;
import io.airbyte.integrations.bicycle.base.integration.job.config.ProducerConfig;
import io.airbyte.integrations.bicycle.base.integration.job.consumer.ConsumerJob;
import io.airbyte.integrations.bicycle.base.integration.job.metrics.EventProcessMetrics;
import io.airbyte.integrations.bicycle.base.integration.job.processor.EventProcessor;
import io.airbyte.integrations.bicycle.base.integration.job.producer.BicycleProducer;
import io.airbyte.integrations.bicycle.base.integration.job.producer.Producer;
import io.airbyte.integrations.bicycle.base.integration.job.producer.ProducerJob;
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
import java.nio.charset.Charset;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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

    private Map<String, List<Long>> fileVsDiscoverRecordNumbers = new ConcurrentHashMap<>();

    private ExecutorService mainExecutorService = Executors.newFixedThreadPool(1, new ThreadFactory() {
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("main-csvconnector-lite-1");
            return t;
        }
    });

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
        boolean success = false;
        try {
            LOGGER.info("SyncData ConnectorConfigManager [{}]", connectorConfigManager);
            initialize(sourceConfig, catalog);
            LOGGER.info("Starting syncdata  [{}] [{}] [{}]", getConnectorId(), sourceConfig, readState);
            int threads = initializeExecutors();
            Status syncStatus = null;
            try {
                syncStatus = getConnectorStatus(SYNC_STATUS);
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalStateException("Failed to fetch the sync state");
            }
            if (syncStatus != null) {
                LOGGER.info("Already preview ingesting records [{}] [{}]", getConnectorId(), syncStatus);
            }
            Map<String, File> files = new HashMap<>();
            if ("true".equalsIgnoreCase(System.getProperty("dev.mode", "false"))) {
                files.put("Payfactory_IQ_AUTHORIZATION_2023_09.json", new File("/home/ravi/Downloads/json/PayFactor-Authorization/Payfactory_IQ_AUTHORIZATION_2023_09.json"));
                files.put("Payfactory_IQ_AUTHORIZATION_2023_10.json", new File("/home/ravi/Downloads/json/PayFactor-Authorization/Payfactory_IQ_AUTHORIZATION_2023_10.json"));
                files.put("Payfactory_IQ_AUTHORIZATION_2023_11.json", new File("/home/ravi/Downloads/json/PayFactor-Authorization/Payfactory_IQ_AUTHORIZATION_2023_11.json"));
            } else {
                List<String> fileNames = new ArrayList<>();
                Map<String, String> fileVsSignedUrls = readFilesConfig();
                LOGGER.info("[{}] : Read Signed files Url [{}]", getConnectorId(), fileVsSignedUrls);
                for (String fileName : fileVsSignedUrls.keySet()) {
                    String status = getConnectorFileState(fileName, SYNC_STATUS);
                    String totalRecords = getConnectorFileState(fileName, SYNC_TOTAL_RECORDS);
                    if (status != null) {
                        fileNames.add(fileName);
                        LOGGER.info("[{}] : syncData removing file fileName [{}] status [{}] records [{}]",
                                getConnectorId(), fileName, status, totalRecords);
                    }
                }
                for (String fileName: fileNames) {
                    fileVsSignedUrls.remove(fileName);
                }
                LOGGER.info("[{}] : Signed files Url [{}]", getConnectorId(), fileVsSignedUrls);
                for (String fileName : fileVsSignedUrls.keySet()) {
                    File file = storeFile(fileName, fileVsSignedUrls.get(fileName));
                    files.put(fileName, file);
                }
            }
            if (files.isEmpty()) {
                LOGGER.info("[{}] : files already synced [{}]", getConnectorId(), files);
                return SyncDataResponse.newBuilder()
                        .setStatus(Status.COMPLETE)
                        .setResponse(StatusResponse.newBuilder().setMessage("SUCCESS").build())
                        .build();
            }
            updateConnectorState(SYNC_STATUS, Status.STARTED, 0);
            LOGGER.info("[{}] : Local files Url [{}]", getConnectorId(), files);
            List<RawEvent> vcEvents = new ArrayList<>();
            for (String fileName : files.keySet()) {
                File file = files.get(fileName);
                EventSourceReader csvReader = null;
                try {
                    csvReader = null;
                    try {
                        csvReader = getReader(fileName, file, getConnectorId(), this, SYNC_DATA);
                        publishPreviewEvents(fileVsDiscoverRecordNumbers, fileName, file, csvReader, vcEvents, PREVIEW_RECORDS,
                                1, 0, false, true, true, false);
                    } finally {
                        if (csvReader != null) {
                            csvReader.close();
                        }
                    }
                } catch (Throwable t) {
                    throw new IllegalStateException("Failed to register preview events for discovery service ["+fileName+"]", t);
                }
            }
            try {
                mainExecutorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        Map<String, Future> futures = processFiles(threads, files);
                    }
                });
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
            success = true;
            return SyncDataResponse.newBuilder()
                    .setStatus(Status.COMPLETE)
                    .setResponse(StatusResponse.newBuilder().setMessage("SUCCESS").build())
                    .build();
        } finally {
            LOGGER.info("Finished syncdata [{}] [{}]", getConnectorId(), success);
        }
    }

    private SyncDataResponse validateFileFormats(Map<String, File> files) {
        List<UnsupportedFormatException> unsupportedFormatExceptions = new ArrayList<>();
        for (String fileName : files.keySet()) {
            File file = files.get(fileName);
            EventSourceReader csvReader = null;
            try {
                csvReader = null;
                try {
                    csvReader = getReader(fileName, file, getConnectorId(), this, SYNC_DATA);
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

    private Map<String, Future> processFiles(int threads, Map<String, File> files) {
        BaseEventConnector connector = this;
        int batchSize = getBatchSize(config);
        int queueSize = getQueueSize(config);
        int requestSize = getRequestSize(config);
        long totalRecords = calculateTotalrecords(files, batchSize);
        updateConnectorState(SYNC_STATUS, Status.IN_PROGRESS, 0);

        String connectorId = getConnectorId();
        ConsumerConfig consumerConfig = ConsumerConfig.newBuilder().setName("preview-events")
                .setIdentifier(connectorId)
                .setPoolSize(threads)
                .setMaxIdlePollsRetries(300)
                .setSleepTimeInMillis(60);
        ProducerConfig producerConfig = ProducerConfig.newBuilder().setName("preview-events")
                .setIdentifier(connectorId)
                .setPoolSize(threads)
                .setMaxBlockingQueueSize(threads * queueSize)
                .setMaxRetries(1000)
                .setSleepTimeInMillis(60);


        Map<String, Future> futures = new HashMap<>();
        EventProcessor<WrapperEvent> eventsProcessor = new EventProcessor(producerConfig, consumerConfig,
                new EventProcessMetrics(totalRecords));

        AtomicLong validCount = new AtomicLong(0);
        AtomicLong invalidCount = new AtomicLong(0);
        AtomicBoolean finished = new AtomicBoolean(false);
        long batch = totalRecords / 20;
        long logbatch = batch > 1000 ? 1000 : batch;

        for (int i = 0; i < threads; i++) {
            LOGGER.info("[{}] : Creating Consumers [{}]", getConnectorId(), i);
            eventsProcessor.submit(new ConsumerJob<WrapperEvent>() {

                private List<RawEvent> validEvents = new ArrayList<>();
                private List<RawEvent> inValidEvents = new ArrayList<>();
                private AtomicLong bufferSize = new AtomicLong(0);
                private ReentrantLock lock = new ReentrantLock();
                private Status status = Status.COMPLETE;

                public void process(WrapperEvent wrapperEvent) {
                    try {
                        boolean acquired = lock.tryLock(60, TimeUnit.SECONDS);
                        RawEvent rawEvent = wrapperEvent.getRawEvent();
                        if (wrapperEvent.isValidEvent()) {
                            validEvents.add(rawEvent);
                            validCount.incrementAndGet();
                        } else {
                            status = Status.ERROR;
                            inValidEvents.add(rawEvent);
                            invalidCount.incrementAndGet();
                        }

                        JsonRawEvent jsonRawEvent = (JsonRawEvent) rawEvent;
                        byte[] bytes = jsonRawEvent.getJsonEvent().getJsonStr().getBytes();
                        bufferSize.addAndGet(bytes.length);
                        if (bufferSize.get() >= requestSize) {
                            publishPreviewEvents(acquired);
                        }
                    } catch (Exception e) {
                        LOGGER.error("[{}] Failed while publishing records [{}] [{}]", validCount.get(), invalidCount.get(), e);
                    } finally {
                        lock.unlock();
                    }
                }

                private void publishPreviewEvents(boolean lockAcquired) {
                    submitRecordsToPreviewStore(getConnectorId(), validEvents, false);
                    if (validCount.get() % logbatch == 0 || finished.get()) {
                        LOGGER.info("[{}] : Raw events total - published count Valid[{}] Invalid[{}] Lock [{}]",
                                getConnectorId(), validCount.get(), invalidCount.get(), lockAcquired);
                    }
                    validEvents.clear();
                    bufferSize.set(0);
                    updateConnectorState(SYNC_STATUS, Status.IN_PROGRESS,
                            (double) validCount.get()/ (double) totalRecords);

                    submitRecordsToPreviewStoreWithMetadata(getConnectorId(), inValidEvents);
                    inValidEvents.clear();
                }

                public void finish() {
                    try {
                        boolean acquired = lock.tryLock(60, TimeUnit.SECONDS);
                        finished.set(true);
                        publishPreviewEvents(acquired);
                        updateConnectorState(SYNC_STATUS, status);
                        LOGGER.info("[{}] : Raw events total final - published count Valid[{}] Invalid[{}]",
                                getConnectorId(), validCount.get(), invalidCount.get());
                    } catch (InterruptedException e) {
                        LOGGER.error("Lock Interrupted", e);
                    } finally {
                        lock.unlock();
                    }
                }
            });
            LOGGER.info("[{}] : Created Consumers", getConnectorId());
        }

        for (String fileName : files.keySet()) {
            LOGGER.info("[{}] : Creating Producers [{}]", getConnectorId(), fileName);
            File file = files.get(fileName);
            Future future = eventsProcessor.submit(new ProducerJob<WrapperEvent>() {

                AtomicLong recordsCount = new AtomicLong(0);
                public void process(Producer<WrapperEvent> producer) {
                    try {
                        EventSourceReader<RawEvent> reader = null;
                        try {
                            updateConnectorFileState(fileName, SYNC_STATUS, Status.IN_PROGRESS.name());
                            List<Long> recordOffsets = fileVsDiscoverRecordNumbers.get(fileName);
                            reader = getReader(fileName, file, getConnectorId(), connector, READ);
                            while (reader.hasNext()) {
                                RawEvent next = reader.next();
                                if (recordOffsets != null && recordOffsets.contains(reader.getOffset())) {
                                    LOGGER.info("[{}] : Ignoring the record [{}] [{}]", getConnectorId(), fileName,
                                            reader.getOffset());
                                    continue;
                                }
                                producer.produce(new WrapperEvent(next, reader.isValidEvent()));
                                recordsCount.incrementAndGet();
                            }
                            updateConnectorFileState(fileName, SYNC_TOTAL_RECORDS, String.valueOf(recordsCount.get()));
                            LOGGER.info("[{}] : Processing file done [{}] [{}]", getConnectorId(), fileName,
                                                recordsCount.get());
                        } finally {
                            if (reader != null) {
                                reader.close();
                            }
                        }
                    } catch (Throwable t) {
                        LOGGER.error("Failed parse file [{}] [{}]", getConnectorId(), fileName, t);
                    }
                }

                public void finish() {
                    updateConnectorFileState(fileName, SYNC_STATUS, Status.COMPLETE.name());
                    LOGGER.info("[{}] : Processing file complete [{}] [{}]", getConnectorId(), fileName,
                            recordsCount.get());
                }
            });
            futures.put(fileName, future);
            LOGGER.info("[{}] : Created Producers [{}]", getConnectorId(), fileName);
        }
        //eventsProcessor.stop();
        //eventsProcessor.shutdown();
        return futures;
    }

    static class WrapperEvent {
        private boolean validEvent;
        private RawEvent rawEvent;

        public WrapperEvent(RawEvent rawEvent, boolean validEvent) {
            this.rawEvent = rawEvent;
            this.validEvent = validEvent;
        }

        public boolean isValidEvent() {
            return validEvent;
        }

        public RawEvent getRawEvent() {
            return rawEvent;
        }
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
            if ("true".equalsIgnoreCase(System.getProperty("dev.mode", "false"))) {
                files.put("DemoData_Feb04toMar11_2024_FixedDateFormat.csv", new File("/home/ravi/Downloads/system-error/export-8dc3fbdf3ee9aff.csv"));
            } else {
                Map<String, String> fileVsSignedUrls = readFilesConfig();
                LOGGER.info("[{}] : Read Signed files Url [{}]", getConnectorId(), fileVsSignedUrls);
                List<String> fileNames = new ArrayList<>();
                for (String fileName : fileVsSignedUrls.keySet()) {
                    String readStatus = getConnectorFileState(fileName, READ_STATUS);
                    String records = getConnectorFileState(fileName, READ_TOTAL_RECORDS);
                    if (readStatus != null) {
                        fileNames.add(fileName);
                        LOGGER.info("[{}] : doRead removing file fileName [{}] status [{}] records [{}]",
                                getConnectorId(), fileName, readStatus, records);
                    }
                }

                for (String fileName: fileNames) {
                    fileVsSignedUrls.remove(fileName);
                }
                LOGGER.info("[{}] : doRead Signed files Url [{}]", getConnectorId(), fileVsSignedUrls);
                for (String fileName : fileVsSignedUrls.keySet()) {
                    File file = storeFile(fileName, fileVsSignedUrls.get(fileName));
                    files.put(fileName, file);
                }
            }

            if (files.isEmpty()) {
                success = true;
                LOGGER.info("[{}] : doRead no files to read [{}]", getConnectorId());
                return null;
            }
            updateConnectorState(READ_STATUS, Status.IN_PROGRESS, 0);
            int queueSize = getQueueSize(config);
            int requestSize = getRequestSize(config);
            long totalRecords = calculateTotalrecords(files, queueSize);
            saveState(TOTAL_RECORDS, totalRecords);
            try {
                long processed = publishEvents(files, queueSize, requestSize, threads, totalRecords);
                updateConnectorState(READ_STATUS, Status.COMPLETE);
                for (String fileName : files.keySet()) {
                    updateConnectorFileState(fileName, READ_STATUS, Status.COMPLETE.name());
                }
                //saveState(TOTAL_RECORDS, totalRecords);
                if (processed > 0) {
                    success = true;
                }
            } catch (Exception e) {
                LOGGER.error("Failed to process records ["+getConnectorId()+"]", e);
                updateConnectorState(READ_STATUS, Status.ERROR);
            }
        } catch (Throwable e) {
            throw new IllegalStateException("Failed to run read ["+getConnectorId()+"]", e);
        } finally {
            if (success) {
                LOGGER.info("doRead Success");
            } else {
                LOGGER.info("doRead Failed");
            }
            stopEventConnector();
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
        String connectorId = getConnectorId();
        ConsumerConfig consumerConfig = ConsumerConfig.newBuilder().setName("bicycle-events-processor")
                .setIdentifier(connectorId)
                .setPoolSize(threads)
                .setMaxIdlePollsRetries(2000)
                .setSleepTimeInMillis(60);
        ProducerConfig producerConfig = ProducerConfig.newBuilder().setName("bicycle-events-processor")
                .setIdentifier(connectorId)
                .setPoolSize(1)
                .setMaxBlockingQueueSize(threads * queueSize)
                .setMaxRetries(1000)
                .setSleepTimeInMillis(60);
        EventProcessor<EventProcessorResult> eventsProcessor
                = new EventProcessor<>(producerConfig, consumerConfig, metrics);
        for (int i = 0; i < threads; i++) {
            eventsProcessor.submit(getPublisherConsumerJob(metrics));
        }

        ConsumerConfig consumerConfig1 = ConsumerConfig.newBuilder().setName("bicycle-rules-processor")
                .setIdentifier(connectorId)
                .setPoolSize(threads)
                .setMaxIdlePollsRetries(2000)
                .setSleepTimeInMillis(60);
        ProducerConfig producerConfig1 = ProducerConfig.newBuilder().setName("bicycle-rules-processor")
                .setIdentifier(connectorId)
                .setPoolSize(1)
                .setMaxBlockingQueueSize(threads * queueSize)
                .setMaxRetries(1000)
                .setSleepTimeInMillis(60);
        EventProcessor<RawEvent> rulesProcessor = new EventProcessor<>(producerConfig1, consumerConfig1, metrics);
        for (int i = 0; i < threads; i++) {
            rulesProcessor.submit(getRulesConsumerJob(requestSize, metrics, eventsProcessor.getProducer()));
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
                EventSourceReader<RawEvent> reader = null;
                long count = 0;
                try {
                    reader = getReader(fileName, file, getConnectorId(), connector, READ);
                    updateConnectorFileState(fileName, READ_STATUS, Status.IN_PROGRESS.name());
                    while (reader.hasNext()) {
                        RawEvent rawEvent = reader.next();
                        producer.produce(rawEvent);
                        count++;
                    }
                    updateConnectorFileState(fileName, READ_TOTAL_RECORDS, String.valueOf(count));
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

    private ConsumerJob<RawEvent> getRulesConsumerJob(int requestSize, EventProcessMetrics metrics,
                                                      BicycleProducer<EventProcessorResult> producer) {

        AtomicLong records = new AtomicLong(0);
        AtomicLong records1 = new AtomicLong(0);
        return new ConsumerJob<RawEvent>() {
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
                    byte[] bytes = jsonRawEvent.getJsonEvent().getJsonStr().getBytes(Charset.defaultCharset());
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
                long totalRecords = metrics.getTotalRecords();
                long batch = totalRecords / 20;
                long logbatch = batch > 1000 ? 1000 : batch;

                EventSourceInfo eventSourceInfo = new EventSourceInfo(getConnectorId(), getEventSourceType());
                EventProcessorResult eventProcessorResult = convertRawEventsToBicycleEvents(getAuthInfo(),
                        eventSourceInfo, rawEvents, getUserServiceMappingRules());
                //Since for CSV lite connector we have already published all the data as preview records,
                //we need to publish any more preview records.
                if (eventProcessorResult != null) {
                    eventProcessorResult.getUnmatchedRawEvents().clear();
                    eventProcessorResult.getMatchedRawEventsForPreview().clear();
                }
                producer.addToQueue(eventProcessorResult);
                if (records.get() % logbatch == 0) {
                    LOGGER.info("[{}] Successfully Pushed Records [{}] [{}] [{}]", getConnectorId(),
                            Thread.currentThread().getName(), records.incrementAndGet(),
                            records1.addAndGet(eventProcessorResult.getUserServiceDefs().size()));
                }
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
        String connectorId = getConnectorId();

        long totalRecords = metrics.getTotalRecords();
        long batch = totalRecords / 20;
        long logbatch = batch > 1000 ? 1000 : batch;

        AtomicLong records = new AtomicLong(0);
        AtomicLong counter = new AtomicLong(0);
        AtomicBoolean finished = new AtomicBoolean(false);
        return new ConsumerJob<EventProcessorResult>() {

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
                    if (metrics.getSuccess() % logbatch == 0 || finished.get()) {
                        LOGGER.info("[{}] Success published records [{}] [{}] batch [{}] counter [{}] time [{}]",
                                connectorId, metrics.getSuccess(), metrics.getFailed(), records.get(), counter.get(),
                                (System.currentTimeMillis() - startTimeInMillis));
                    }
                    updateConnectorState(READ_STATUS, Status.IN_PROGRESS,
                                        ((double) metrics.getSuccess() / (double) metrics.getTotalRecords()));
                }
            }

            public void finish() {
                LOGGER.info("[{}] Success published records final success[{}] failed[{}] total[{}]", connectorId,
                        metrics.getSuccess(), metrics.getFailed(), metrics.getTotalRecords());
                finished.set(true);
            }
        };
    }

    private long calculateTotalrecords(Map<String, File> files, int batchSize) {
        long totalRecords = getStateAsLong(TOTAL_RECORDS);
        if (totalRecords == -1) {
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
            totalRecords = successCounter.get();
            int retries = 0;
            do {
                try {
                    saveState(TOTAL_RECORDS, totalRecords);
                    break;
                } catch (JsonProcessingException e) {
                    LOGGER.error("[{}] Error updating total records [{}]", getConnectorId(), retries, e);
                    retries++;
                }
            } while (retries < 10);
            throw new IllegalStateException("["+getConnectorId()+"] Failed to update total records ["+totalRecords+"]");
        }
        return totalRecords;
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
