package io.airbyte.integrations.source.event.google.sheets;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.inception.server.auth.api.SystemAuthenticator;
import com.inception.server.auth.model.AuthInfo;
import com.inception.server.scheduler.api.JobExecutionStatus;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.EventConnectorJobStatusNotifier;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.SyncMode;
import io.bicycle.integration.common.config.manager.ConnectorConfigManager;
import io.bicycle.integration.connector.SyncDataRequest;
import io.bicycle.integration.connector.SyncDataResponse;
import io.bicycle.integration.connector.runtime.BackFillConfiguration;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The purpose of this connector is to serve the sync data for push based connectors as we don't
 * actually run push based connectors in our system.
 */

public class GoogleSheetsEventConnector extends BaseEventConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleSheetsEventConnector.class);


    public GoogleSheetsEventConnector(SystemAuthenticator systemAuthenticator,
                                      EventConnectorJobStatusNotifier eventConnectorJobStatusNotifier,
                                      ConnectorConfigManager connectorConfigManager) {
        super(systemAuthenticator, eventConnectorJobStatusNotifier, connectorConfigManager);
    }

    protected int getTotalRecordsConsumed() {
        return 0;
    }

    public void stopEventConnector() {
        stopEventConnector("Successfully Stopped", JobExecutionStatus.success);
    }

    @Override
    public List<RawEvent> convertRecordsToRawEventsInternal(List<?> records) {
        return null;
    }

    public AutoCloseableIterator<AirbyteMessage> preview(JsonNode config, ConfiguredAirbyteCatalog catalog,
                                                         JsonNode state)
            throws InterruptedException, ExecutionException {
        GoogleSheetReadUtil googleSheetReadUtil = new GoogleSheetReadUtil();
        LOGGER.info("Preview for google sheet connector with config {} and catalog {}", config, catalog);
        try {
            Spreadsheet spreadsheet = getSpreadSheet(googleSheetReadUtil, config);
            ConfiguredAirbyteStream configuredAirbyteStream = (ConfiguredAirbyteStream) catalog.getStreams().get(0);
            String streamName = configuredAirbyteStream.getStream().getName();
            String url = config.get(GoogleSheetConstants.CONFIG_SPREAD_SHEET_ID).asText();
            JsonNode credential = config.get(GoogleSheetConstants.CONFIG_CREDENTIALS);
            String authType = credential.get(GoogleSheetConstants.CONFIG_AUTH_TYPE).asText();
            String token = credential.get(GoogleSheetConstants.CONFIG_CREDENTIALS_JSON).asText();
            String spreadSheetId = getSpreadsheetIdFromUrl(url);
            LOGGER.info("SpreadSheetId {} for stream {}", spreadSheetId, streamName);
            List<JsonNode> jsonNodes = readSheetForPreview(spreadsheet, streamName, token, spreadSheetId,
                    googleSheetReadUtil);

            Iterator<JsonNode> iterator = jsonNodes.iterator();

            return AutoCloseableIterators.fromIterator(new AbstractIterator<>() {

                @Override
                protected AirbyteMessage computeNext() {
                    if (iterator.hasNext()) {
                        final JsonNode record = iterator.next();
                        return new AirbyteMessage()
                                .withType(AirbyteMessage.Type.RECORD)
                                .withRecord(new AirbyteRecordMessage()
                                        .withStream(streamName)
                                        .withEmittedAt(Instant.now().toEpochMilli())
                                        .withData(record));
                    }

                    return endOfData();
                }

            });

        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public SyncDataResponse syncData(JsonNode sourceConfig, ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                                     JsonNode readState, SyncDataRequest syncDataRequest) {
        return SyncDataResponse.getDefaultInstance();
    }

    public AirbyteConnectionStatus check(JsonNode config) throws Exception {

        LOGGER.info("Check for google sheet connector with config {}", config);

        try {
            GoogleSheetReadUtil googleSheetReadUtil = new GoogleSheetReadUtil();
            getSpreadSheet(googleSheetReadUtil, config);
            return new AirbyteConnectionStatus()
                    .withStatus(AirbyteConnectionStatus.Status.SUCCEEDED)
                    .withMessage("Success");
        } catch (Exception e) {
            return new AirbyteConnectionStatus()
                    .withStatus(AirbyteConnectionStatus.Status.FAILED)
                    .withMessage(e.getMessage());
        }
    }

    public AirbyteCatalog discover(JsonNode config) throws Exception {
        LOGGER.info("Discover google sheet connector with config {}", config);

        try {
            GoogleSheetReadUtil googleSheetReadUtil = new GoogleSheetReadUtil();
            Spreadsheet spreadsheet = getSpreadSheet(googleSheetReadUtil, config);
            List<Sheet> sheets = spreadsheet.getSheets();
            final List<AirbyteStream> streams = sheets.stream().map(sheet -> CatalogHelpers
                            .createAirbyteStream(sheet.getProperties().getTitle(), Field.of("value", JsonSchemaType.STRING))
                            .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL)))
                    .collect(Collectors.toList());
            if (streams.size() > 1) {
                streams.add(CatalogHelpers.createAirbyteStream("All",
                                Field.of("value", JsonSchemaType.STRING))
                        .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL)));
            }
            return new AirbyteCatalog().withStreams(streams);
        } catch (Exception e) {
            LOGGER.error("Unable to test connection for ");
            throw e;
        }
    }

    public AutoCloseableIterator<AirbyteMessage> doRead(
            JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) throws Exception {

        LOGGER.info("Read google sheet connector with config {}, catalog {}, state {}", config, catalog, state);
        ConfiguredAirbyteStream configuredAirbyteStream = catalog.getStreams().get(0);
        String streamName = configuredAirbyteStream.getStream().getName();
        String connectorId = getConnectorId();
        String eventSourceType = getEventSourceType();

        AuthInfo authInfo = getAuthInfo();

        String url = config.get(GoogleSheetConstants.CONFIG_SPREAD_SHEET_ID).asText();
        JsonNode credential = config.get(GoogleSheetConstants.CONFIG_CREDENTIALS);
        String authType = credential.get(GoogleSheetConstants.CONFIG_AUTH_TYPE).asText();
        String token = credential.get(GoogleSheetConstants.CONFIG_CREDENTIALS_JSON).asText();
        String trackingColumnName = config.has(GoogleSheetConstants.CONFIG_READ_TRACKING_COLUMN) ?
                config.get(GoogleSheetConstants.CONFIG_READ_TRACKING_COLUMN).asText() :
                GoogleSheetConstants.READ_TRACKING_COLUMN_DEFAULT_VALUE;
        String trackingColumnPattern = config.has(GoogleSheetConstants.CONFIG_READ_TRACKING_COLUMN_PATTERN) ?
                config.get(GoogleSheetConstants.CONFIG_READ_TRACKING_COLUMN_PATTERN).asText():
                GoogleSheetConstants.READ_TRACKING_COLUMN_FORMAT_DEFAULT_VALUE;

        ConnectorConfigManager configManager = getConnectorConfigManager();
        LOGGER.info("{} connector config manager {}", connectorId, configManager);

        runtimeConfig = configManager.getRuntimeConfig(authInfo, connectorId);
        BackFillConfiguration backFillConfig = runtimeConfig.getBackFillConfig();
        boolean isBackFillEnabled = backFillConfig.getEnableBackFill();
        long backFillStartTime = backFillConfig.getStartTimeInMillis() == 0 ? -1 :
                backFillConfig.getStartTimeInMillis();
        long backFillEndTime = backFillConfig.getEndTimeInMillis() == 0 ? -1 :
                backFillConfig.getEndTimeInMillis();

        GoogleSheetReadUtil googleSheetReadUtil = new GoogleSheetReadUtil(this, connectorId,
                streamName, eventSourceType, token, backFillStartTime, backFillEndTime);

        Spreadsheet spreadsheet = getSpreadSheet(googleSheetReadUtil, config);
        String spreadSheetId = getSpreadsheetIdFromUrl(url);


        boolean publishedEvents = googleSheetReadUtil.readSheet(spreadsheet, spreadSheetId, trackingColumnName,
                trackingColumnPattern);

        long dummyMessageInterval = 600;

        if (publishedEvents && isBackFillEnabled) {
            LOGGER.info("Starting publishing dummy events for stream Id {}", connectorId);
            publishDummyEvents(authInfo, eventSourceInfo, dummyMessageInterval);
            LOGGER.info("Done publishing dummy events for stream Id {}", connectorId);
        }

        stopEventConnector();

        return null;
    }

    public String getSpreadsheetIdFromUrl(String url) {
        // Regular expression pattern to match the spreadsheet ID
        Pattern pattern = Pattern.compile("/d/(.*?)/");
        Matcher matcher = pattern.matcher(url);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null; // URL format not recognized
    }

    private Spreadsheet getSpreadSheet(GoogleSheetReadUtil googleSheetReadUtil, JsonNode config) throws Exception {

        try {
            String url = config.get(GoogleSheetConstants.CONFIG_SPREAD_SHEET_ID).asText();
            JsonNode credential = config.get(GoogleSheetConstants.CONFIG_CREDENTIALS);
            String authType = credential.get(GoogleSheetConstants.CONFIG_AUTH_TYPE).asText();
            String token = credential.get(GoogleSheetConstants.CONFIG_CREDENTIALS_JSON).asText();
            Sheets sheetsService = googleSheetReadUtil.getSheetsService(token);
            String spreadSheetId = getSpreadsheetIdFromUrl(url);
            Spreadsheet spreadsheet = sheetsService.spreadsheets().get(spreadSheetId).execute();
            return spreadsheet;
        } catch (Exception e) {
            LOGGER.error("Unable get spread sheet", e);
            throw e;
        }
    }

    private List<JsonNode> readSheetForPreview(Spreadsheet spreadsheet, String streamName, String credentialJson,
                                               String spreadSheetId,
                                               GoogleSheetReadUtil googleSheetReadUtil)
            throws IOException, GeneralSecurityException {

        int limit = 100;
        List<JsonNode> jsonNodes = new ArrayList<>();

        try {
            Sheets sheetsService = googleSheetReadUtil.getSheetsService(credentialJson);
            for (Sheet sheet : spreadsheet.getSheets()) {
                String sheetName = sheet.getProperties().getTitle();

                if (!streamName.equals("ALL") && !streamName.equals(sheetName)) {
                    continue;
                }

                // Fetch all data from the sheet
                ValueRange response = sheetsService.spreadsheets().values()
                        .get(spreadSheetId, sheetName)
                        .execute();
                // Process the data
                List<List<Object>> values = response.getValues();
                if (values != null && !values.isEmpty()) {
                    List<JsonNode> jsonRows = googleSheetReadUtil.convertSheetDataToJson(sheetName, values, limit);
                    limit = limit - jsonRows.size();
                    jsonNodes.addAll(jsonRows);
                }

                if (limit <= 0) {
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.error("Unable to read data for preview for stream {} {}", streamName, e);
            throw e;
        }

        return jsonNodes;
    }


}
