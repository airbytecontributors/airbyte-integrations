package io.airbyte.integrations.bicycle.base.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.inception.common.client.ServiceLocator;
import com.inception.common.client.impl.GenericApiClient;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.net.URL;
import java.util.*;

import static io.bicycle.utils.metric.MetricNameConstants.PREVIEW_EVENTS_PUBLISH_NETWORK_CALL_TIME;

public abstract class BaseCSVEventConnector extends BaseEventConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseCSVEventConnector.class);
    private static final int CONNECT_TIMEOUT_IN_MILLIS = 60000;
    private static final int READ_TIMEOUT_IN_MILLIS = 60000;
    private static final int BATCH_SIZE = 30;
    protected BlobStoreBroker blobStoreBroker;
    protected BlobStoreClient blobStoreClient;
    protected PreviewEventSamplingHandler previewEventSamplingHandler;
    protected PreviewStoreClient previewStoreClient;
    protected CommonUtil commonUtil = new CommonUtil();
    private ObjectMapper mapper = new ObjectMapper();

    protected ConnectorConfigService connectorConfigService;

    private Map<String, Integer> headerNameToIndexMap;

    public BaseCSVEventConnector(SystemAuthenticator systemAuthenticator,
                                 EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier,
                                 ConnectorConfigManager connectorConfigManager) {
        super(systemAuthenticator, eventConnectorJobStatusNotifier, connectorConfigManager);
    }

    protected void initialize(JsonNode config, ConfiguredAirbyteCatalog catalog) {
        this.config = config;
        this.additionalProperties = catalog.getAdditionalProperties();
        this.blobStoreBroker = new BlobStoreBroker(getBlobStoreClient());
        this.state = getStateAsJsonNode(getAuthInfo(), getConnectorId());
        BicycleConfig bicycleConfig = getBicycleConfig(additionalProperties, systemAuthenticator);
        setBicycleEventProcessorAndPublisher(bicycleConfig);
        getConnectionServiceClient();
        this.connectorConfigService = new ConnectorConfigServiceImpl(configStoreClient, schemaStoreApiClient,
                entityStoreApiClient, null, null,
                null, systemAuthenticator, blobStoreBroker, null, null);
    }

    protected BlobStoreClient getBlobStoreClient() {
        if (blobStoreClient == null) {
            String serverUrl = getBicycleServerURL();
            if (serverUrl == null || serverUrl.isEmpty()) {
                throw new IllegalStateException("Bicycle server url is null");
            }
            ServiceLocator serviceLocator = new ServiceLocator() {
                @Override
                public String getBaseUri() {
                    return serverUrl;
                }
            };

            blobStoreClient = new BlobStoreApiClient(new GenericApiClient(), serviceLocator);
        }

        return blobStoreClient;
    }

    protected List<CSVRecord> parseCSVFiles(File csvFile) throws IOException {
        String streamId = getConnectorId();
        List<CSVRecord> mismatchRecords = new ArrayList<>();
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
                        if (headerNameToIndexMap.size() != csvRecord.size()) {
                            mismatchRecords.add(csvRecord);
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
        return mismatchRecords;
    }

    protected long updateFilesMetadata(File csvFile, int recordsCount, boolean updateVC) throws IOException {
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
                            submitRecordsToPreviewStore(getConnectorId()+"-sanity", sanityRawEvents, true);
                            sanityRawEvents.clear();
                        }
                        if (rawEvents.size() >= BATCH_SIZE) {
                            submitRecordsToPreviewStore(getConnectorId(), rawEvents, false);
                            if (updateVC) {
                                updateTenantSummaryVC(getAuthInfo(), "", "", rawEvents, getConnectorId(), "");
                                updateVC = false;
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

    protected void submitRecordsToPreviewStore(String eventSourceId, List<RawEvent> rawEvents, boolean shouldFlush) {
        String eventSourceType = getEventSourceType(additionalProperties);
        EventSourceInfo eventSourceInfo = new EventSourceInfo(eventSourceId, eventSourceType);
        bicycleEventPublisher.publishPreviewEvents(getAuthInfo(), eventSourceInfo, rawEvents, shouldFlush);
        LOGGER.info("[{}] : Published preview events [{}] [{}]", getConnectorId(), eventSourceId,
                shouldFlush);
    }



    private void updateTenantSummaryVC(AuthInfo authInfo, String traceId, String companyName,
                                       List<RawEvent> rawEvents,
                                       String configId, String configName) {
        try {
            VerticalIdentifier verticalIdentifier = getVerticalIdentifier(companyName);

            RawDataKnowledgeBase.Builder rawDataKnowledgeBaseBuilder = RawDataKnowledgeBase.newBuilder();
            KnowledgeBaseMetadata knowledgeBaseMetadata = KnowledgeBaseMetadata.newBuilder()
                    .setCompanyName(companyName)
                    .setSource(Source.RAW_DATA)
                    .setSourceId(configId)
                    .setSourceName(configName)
                    .setConnectorId(configId)
                    .build();

            rawDataKnowledgeBaseBuilder.setMetaData(knowledgeBaseMetadata);
            rawDataKnowledgeBaseBuilder.setVerticalIdentifier(verticalIdentifier);

            Map<String, List<String>> fieldsVsSamples = new HashMap<>();
            for (RawEvent rawEvent : rawEvents) {
                ObjectNode objectNode = (ObjectNode) rawEvent.getRawEventObject();
                objectNode.fields().forEachRemaining(entry -> {
                    String key = entry.getKey();
                    JsonNode value = entry.getValue();
                    fieldsVsSamples.computeIfAbsent(key, (k) -> new ArrayList<>()).add(value.textValue());
                });
            }

            RawDataSource.Builder rawDataSourceBuilder = RawDataSource.newBuilder();
            //String topicName = rawEvent.getEventTypeName();
            //rawDataSourceBuilder.setSourceName(topicName);
            for (String fieldName : fieldsVsSamples.keySet()) {
                if (StringUtils.isEmpty(fieldName)) {
                    continue;
                }
                rawDataSourceBuilder.addDimensions(RawDataField.newBuilder()
                        .setFieldName(fieldName)
                        .addAllSampleValues(fieldsVsSamples.get(fieldName))
                        .build());
            }
            rawDataKnowledgeBaseBuilder.addRawDataSource(rawDataSourceBuilder.build());



            DiscoverTenantSummary.Builder builder = DiscoverTenantSummary.newBuilder();
            builder.setVertical(verticalIdentifier);
            builder.setRawDataKnowledgeBase(rawDataKnowledgeBaseBuilder.build());
            builder.setTraceInfo(io.bicycle.server.verticalcontext.tenant.api.TraceInfo
                    .newBuilder().setTraceId(traceId).build());

            DiscoverTenantSummaryResponse response =
                    tenantSummaryDiscovererClient.discoverTenantSummary(authInfo, builder.build());

            LOGGER.info("{} Response from tenant summary discoverer {}", traceId, response);
        } catch (Exception e) {
            LOGGER.error("{} Unable to update tenant summary for company {} because of {}", traceId, companyName, e);
        }
    }

    private VerticalIdentifier getVerticalIdentifier(String companyName) {
        return VerticalIdentifier.newBuilder().setVertical(companyName).setCompanyName(companyName).build();
    }

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

    protected File storeFile(String fileName, String signedUrl) {
        try {
            File file = File.createTempFile(UUID.randomUUID().toString(), ".csv");
            file.deleteOnExit();
            final JsonNode provider = config.get("provider");

            if (provider.get("storage").asText().equals("GCS")) {
                //csvConnector.storeToFile(config, file);
            } else {
                FileUtils.copyURLToFile(new URL(signedUrl), file, CONNECT_TIMEOUT_IN_MILLIS, READ_TIMEOUT_IN_MILLIS);
            }
            return file;
        } catch (Exception e) {
            throw new RuntimeException("Unable to read file from GCS", e);
        }
    }

    protected Map<String, String> readFilesConfig() {
        Map<String, String> fileNameVsSignedUrl = new HashMap<>();
        String traceInfo = "";
        ConfiguredConnectorStream connectorStream = getConfiguredConnectorStream(getAuthInfo(), getConnectorId());
        Pair namespaceAndUploadIds = commonUtil.getNameSpaceToUploadIdsForKnowledgeBaseConnector(traceInfo, connectorStream);
        LOGGER.info("{} Fetch the namespace and uploadIds {}", traceInfo, namespaceAndUploadIds);
        if (namespaceAndUploadIds != null) {
            String namespace = (String) namespaceAndUploadIds.getLeft();
            Collection<String> uploadIds = (Collection<String>) namespaceAndUploadIds.getRight();

            for (String uploadId : uploadIds) {
                BlobObject fileMetadata = getFileMetadata(getAuthInfo(), traceInfo, namespace, uploadId);
                LOGGER.info("{} Got the file metadata {}", traceInfo, fileMetadata);
                String signedUrl = getSingedUrl(getAuthInfo(), traceInfo, namespace, uploadId);
                if (StringUtils.isEmpty(signedUrl)) {
                    LOGGER.warn("{} Unable to get the signed url for file {}", traceInfo,
                            fileMetadata.getName());
                    continue;
                }
                LOGGER.info("{} Got the signed url {} for file {}", traceInfo, signedUrl,
                        fileMetadata.getName());
                fileNameVsSignedUrl.put(fileMetadata.getName(), signedUrl);
                //LOGGER.info("{} Got the file content {}", traceInfo, fileContent);
            }
        }
        return fileNameVsSignedUrl;
    }

    protected ConfiguredConnectorStream getConfiguredConnectorStream(AuthInfo authInfo, String configurationId) {
        return connectorConfigService.getConnectorStreamConfigById(authInfo, configurationId);
    }

    private BlobObject getFileMetadata(AuthInfo authInfo, String traceInfo,
                                       String namespace, String uploadId) {
        return blobStoreBroker.getFileMetadata(authInfo, traceInfo, uploadId, namespace);
    }

    private String getFileContent(String traceInfo, String signedUrl) {
        return blobStoreBroker.getUploadFileContentAsString(traceInfo, signedUrl);
    }

    public String getSingedUrl(AuthInfo authInfo, String traceInfo, String namespace, String connectorUploadId) {
        try {
            return blobStoreBroker.getSingedUrl(authInfo, traceInfo, connectorUploadId, namespace);
        } catch (Throwable var6) {
            String message = "Failed to get signed url from blob store for given bicycle upload id";
            LOGGER.error("{},{} {}", new Object[] {traceInfo, message, connectorUploadId, var6});
        }
        return null;
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
