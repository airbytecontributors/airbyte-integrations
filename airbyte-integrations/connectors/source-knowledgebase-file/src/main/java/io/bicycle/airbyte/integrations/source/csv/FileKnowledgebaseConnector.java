package io.bicycle.airbyte.integrations.source.csv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.config.Config;
import com.inception.server.config.ConfigReference;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.bicycle.base.integration.BaseKnowledgeBaseConnector;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.SyncMode;
import io.bicycle.blob.store.schema.BlobObject;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.integration.common.constants.ConfigConstants;
import io.bicycle.integration.common.utils.CommonUtil;
import io.bicycle.integration.connector.ConfiguredConnectorStream;
import io.bicycle.integration.connector.ErrorResponse;
import io.bicycle.integration.connector.FileKnowledgeBaseConnectorResponse;
import io.bicycle.integration.connector.FileSummary;
import io.bicycle.integration.connector.KnowledgeBaseCompanySummary;
import io.bicycle.integration.connector.KnowledgeBaseConnectorResponse;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:ravi.noothi@agilitix.ai">Ravi Kiran Noothi</a>
 * @since 14/11/22
 */

public class FileKnowledgebaseConnector extends BaseKnowledgeBaseConnector implements Source {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileKnowledgebaseConnector.class);


    public FileKnowledgebaseConnector(SystemAuthenticator systemAuthenticator,
                                      EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier,
                                      ConnectorConfigManager connectorConfigManager) {
        super(systemAuthenticator, eventConnectorJobStatusNotifier, connectorConfigManager);
    }

    @Override
    public AirbyteConnectionStatus check(JsonNode config) throws Exception {
        return new AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.SUCCEEDED);
    }

    @Override
    public AirbyteCatalog discover(JsonNode config) throws Exception {

        final List<AirbyteStream> streams = Collections.singletonList(
                CatalogHelpers.createAirbyteStream("<NA>", Field.of("value", JsonSchemaType.STRING))
                        .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
        );

        return new AirbyteCatalog().withStreams(streams);
    }

    @Override
    protected int getTotalRecordsConsumed() {
        return 0;
    }

    @Override
    public void stopEventConnector() {

    }

    @Override
    public KnowledgeBaseConnectorResponse getKnowledgeBaseConnectorResponseInternal(
            JsonNode config, ConfiguredAirbyteCatalog catalog) throws JsonProcessingException {

        LOGGER.info("Config received {} and additional properties {}", config, additionalProperties);
        KnowledgeBaseConnectorResponse.Builder knowledgeBaseConnRespBuilder =
                KnowledgeBaseConnectorResponse.newBuilder();
        String knowledgeBaseConnectorId = additionalProperties.containsKey("bicycleConnectorId")
                ? additionalProperties.get("bicycleConnectorId").toString() : null;

        if (StringUtils.isEmpty(knowledgeBaseConnectorId)) {
            return getErrorResponse("Connector Id cannot be empty or null");
        }

        String traceInfo = additionalProperties.containsKey("traceId")
                ? additionalProperties.get("traceId").toString() : null;

        AuthInfo authInfo = getAuthInfo();
        LOGGER.info("{} Received request to get knowledge base connector for connector id {}", traceInfo,
                knowledgeBaseConnectorId);

        ConfiguredConnectorStream connectorStream =
                getConfiguredConnectorStream(authInfo, knowledgeBaseConnectorId);

        LOGGER.info("{} Fetch the connector stream {}", traceInfo, connectorStream);

        String connectionConfigurationString = connectorStream.getConfiguredConnection().getConnectionConfiguration();
        JsonNode connectionConfigJson = objectMapper.readTree(connectionConfigurationString);

        if (connectionConfigJson.hasNonNull("data_source") && !connectionConfigJson.get("data_source").isNull()) {
            JsonNode dataSourceConfig = connectionConfigJson.get("data_source");
            LOGGER.info("{} Found data source config {}", traceInfo, dataSourceConfig);
            String sourceType = dataSourceConfig.get("source_type").textValue();
            FileKnowledgeBaseConnectorResponse.Builder fileKnowledgeBaseConnector =
                    FileKnowledgeBaseConnectorResponse.newBuilder();
            if (sourceType.equals("Upload")) {
                Pair namespaceAndUploadIds = getNamespaceAndUploadIds(traceInfo, connectorStream);
                if (namespaceAndUploadIds == null) {
                    return getErrorResponse("Unable to get name space and Id of file, May be file is deleted");
                }
                LOGGER.info("{} Fetch the namespace and uploadIds {}", traceInfo, namespaceAndUploadIds);
                if (namespaceAndUploadIds != null) {
                    String namespace = (String) namespaceAndUploadIds.getLeft();
                    Collection<String> uploadIds = (Collection<String>) namespaceAndUploadIds.getRight();
                    if (uploadIds.isEmpty()) {
                        return getErrorResponse("Unable to get upload ids for files, May be file(s) is deleted");
                    }
                    for (String uploadId : uploadIds) {
                        BlobObject fileMetadata = getFileMetadata(authInfo, traceInfo, namespace, uploadId);
                        LOGGER.info("{} Got the file metadata {}", traceInfo, fileMetadata);
                        String signedUrl = getSingedUrl(authInfo, traceInfo, namespace, uploadId);
                        if (StringUtils.isEmpty(signedUrl)) {
                            LOGGER.warn("{} Unable to get the signed url for file {}", traceInfo,
                                    fileMetadata.getName());
                            continue;
                        }
                        LOGGER.info("{} Got the signed url {} for file {}", traceInfo, signedUrl,
                                fileMetadata.getName());
                        String fileContent = getFileContent(traceInfo, signedUrl);
                        LOGGER.info("{} Got the file content {}", traceInfo, fileContent);
                        FileSummary fileSummary = FileSummary.newBuilder().setContent(fileContent)
                                .setIdentifier(uploadId)
                                .setFileName(fileMetadata.getName())
                                .setLastModifiedTime(System.currentTimeMillis()).build();
                        fileKnowledgeBaseConnector.addFileSummary(fileSummary);
                    }
                }
            } else if (sourceType.equals("Inline Text")) {
                String fileContent = dataSourceConfig.get("text").textValue();
                LOGGER.info("{} Inside inline text with file content size {}", traceInfo, fileContent.length());
                FileSummary fileSummary = FileSummary.newBuilder().setContent(fileContent)
                        .setIdentifier(connectorStream.getConfiguredConnectorStreamId())
                        .setLastModifiedTime(System.currentTimeMillis()).build();
                fileKnowledgeBaseConnector.addFileSummary(fileSummary);
            }

            List<FileSummary> fileSummaries = fileKnowledgeBaseConnector.getFileSummaryList();
            boolean isContentPresent = false;
            for (FileSummary fileSummary: fileSummaries) {
                if (StringUtils.isNotEmpty(fileSummary.getContent())) {
                    isContentPresent = true;
                }
            }

            if (!isContentPresent) {
                return getErrorResponse("File content is empty");
            }

            KnowledgeBaseCompanySummary knowledgeBaseCompanySummary = getCompanySummary(traceInfo, connectorStream);

            KnowledgeBaseConnectorResponse response =
                    knowledgeBaseConnRespBuilder.setCompanySummary(knowledgeBaseCompanySummary)
                            .setFileKnowledgeBaseConnectorResponse(fileKnowledgeBaseConnector.build())
                            .build();

            LOGGER.info("{} response sent from connector is {}", traceInfo, response);

            return response;
        }

        return null;
    }

    private KnowledgeBaseConnectorResponse getErrorResponse(String message) {
        return KnowledgeBaseConnectorResponse.newBuilder()
                .setErrorResponse(ErrorResponse.newBuilder().setError(message).build()).build();
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

    private Pair getNamespaceAndUploadIds(String traceInfo,
                                          ConfiguredConnectorStream connectorStream) {
        return commonUtil.getNameSpaceToUploadIdsForKnowledgeBaseConnector(traceInfo, connectorStream);
    }

    private BlobObject getFileMetadata(AuthInfo authInfo, String traceInfo,
                                       String namespace, String uploadId) {
        return blobStoreBroker.getFileMetadata(authInfo, traceInfo, uploadId, namespace);
    }

    private String getFileContent(String traceInfo, String signedUrl) {
        return blobStoreBroker.getUploadFileContentAsString(traceInfo, signedUrl);
    }

    private KnowledgeBaseCompanySummary getCompanySummary(String traceInfo,
                                                          ConfiguredConnectorStream connectorStream) {

        KnowledgeBaseCompanySummary.Builder builder = KnowledgeBaseCompanySummary.newBuilder();
        try {
            CommonUtil.CompanySummary companySummary = commonUtil.getCompanySummary(traceInfo, connectorStream);
            builder.setCompanyName(companySummary.getCompanyName());
            if (!StringUtils.isEmpty(companySummary.getVertical())) {
                builder.setVerticalName(companySummary.getVertical());
            }
            if (!StringUtils.isEmpty(companySummary.getSubVertical())) {
                builder.setSubVerticalName(companySummary.getSubVertical());
            }
        } catch (Exception e) {
            LOGGER.error("{} Unable to get vertical identifier for stream {} because of {}", traceInfo,
                    connectorStream.getConfiguredConnectorStreamId(), e);
        }
        LOGGER.info("{} got the vertical Identifier {}", traceInfo, builder);
        return builder.build();
    }

    @Override
    public AutoCloseableIterator<AirbyteMessage> read(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state)
            throws Exception {
        return null;
    }

}
