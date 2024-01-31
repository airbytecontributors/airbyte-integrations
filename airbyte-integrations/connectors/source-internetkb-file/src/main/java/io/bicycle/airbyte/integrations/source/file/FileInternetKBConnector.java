package io.bicycle.airbyte.integrations.source.file;

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
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.integration.common.constants.ConfigConstants;
import io.bicycle.integration.common.utils.CommonUtil;
import io.bicycle.integration.connector.ConfiguredConnectorStream;
import io.bicycle.integration.connector.FileKnowledgeBaseConnectorResponse;
import io.bicycle.integration.connector.FileSummary;
import io.bicycle.integration.connector.KnowledgeBaseCompanySummary;
import io.bicycle.integration.connector.KnowledgeBaseConnectorResponse;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileInternetKBConnector extends BaseKnowledgeBaseConnector implements Source {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileInternetKBConnector.class);


    public FileInternetKBConnector(SystemAuthenticator systemAuthenticator,
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
            throw new RuntimeException("Connector Id cannot be empty or null");
        }

        String traceInfo = additionalProperties.containsKey("traceId")
                ? additionalProperties.get("traceId").toString() : null;

        AuthInfo authInfo = getAuthInfo();
        LOGGER.info("{} Received request to get knowledge base connector for connector id {}", traceInfo,
                knowledgeBaseConnectorId);

        ConfiguredConnectorStream connectorStream =
                getConfiguredConnectorStream(authInfo, knowledgeBaseConnectorId);

        Config streamConfig = getConfigByReference(authInfo, ConfigReference.newBuilder()
                .setUuid(knowledgeBaseConnectorId).setTypeId(ConfigConstants.CONNECTOR_STREAM_CONFIG_TYPE).build());


        LOGGER.info("{} Fetch the connector stream {}", traceInfo, connectorStream);

        String connectionConfigurationString = connectorStream.getConfiguredConnection().getConnectionConfiguration();
        JsonNode connectionConfigJson = objectMapper.readTree(connectionConfigurationString);

        final var knowledgeBaseText = "";

        if (connectionConfigJson.hasNonNull("knowledge_base") && !connectionConfigJson.get("knowledge_base").isNull()) {
            final var knowledgeBase = connectionConfigJson.get("knowledge_base");
            final var knowledgeBaseText = knowledgeBase.textValue();
            LOGGER.info("{} Found knowledge base config {}", traceInfo, knowledgeBaseText);
        }


        FileKnowledgeBaseConnectorResponse.Builder fileKnowledgeBaseConnector =
                FileKnowledgeBaseConnectorResponse.newBuilder();
        FileSummary fileSummary =
                FileSummary.newBuilder()
                        .setContent(knowledgeBaseText)
                        .setIdentifier(connectorStream.getConfiguredConnectorStreamId())
                        .setLastModifiedTime(System.currentTimeMillis()).build();
        fileKnowledgeBaseConnector.addFileSummary(fileSummary);

        KnowledgeBaseCompanySummary knowledgeBaseCompanySummary = getCompanySummary(traceInfo, connectorStream);

        KnowledgeBaseConnectorResponse response =
                knowledgeBaseConnRespBuilder
                        .setCompanySummary(knowledgeBaseCompanySummary)
                        .setFileKnowledgeBaseConnectorResponse(fileKnowledgeBaseConnector.build())
                        .build();

        LOGGER.info("{} response sent from connector is {}", traceInfo, response);

        return response;
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

    private KnowledgeBaseCompanySummary getCompanySummary(String traceInfo,
                                                          ConfiguredConnectorStream connectorStream) {

        KnowledgeBaseCompanySummary.Builder builder = KnowledgeBaseCompanySummary.newBuilder();
        try {
            CommonUtil.CompanySummary companySummary = commonUtil.getCompanySummary(traceInfo, connectorStream);
            builder.setCompanyName(companySummary.getCompanyName());
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
