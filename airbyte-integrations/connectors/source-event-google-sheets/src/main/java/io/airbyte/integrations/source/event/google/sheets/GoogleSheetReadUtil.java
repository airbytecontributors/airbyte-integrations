package io.airbyte.integrations.source.event.google.sheets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.inception.server.auth.model.AuthInfo;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sumitmaheshwari
 * Created on 03/03/2024
 */
public class GoogleSheetReadUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleSheetReadUtil.class.getName());
    private static final String DATE_TIME_FORMAT_FALLBACK_PATTERN = "yyyy-MM-dd HH:mm:ss z";
    private static final String APPLICATION_NAME = "BicycleGoogleSheetsEventConnector";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private BaseEventConnector baseEventConnector;
    private String streamName;
    private String streamId;
    private String sourceType;
    private String credentialJson;
    private long backfillStartTimeInMillis = -1;
    private long backfillEndTimeInMillis = -1;
    private int batchSize = 2000;
    private boolean isBackfillEnabled;


    public GoogleSheetReadUtil() {

    }

    //This is used for read command
    public GoogleSheetReadUtil(BaseEventConnector baseEventConnector, String streamId, String streamName,
                               String sourceType, String credentialJson, long backfillStartTimeInMillis,
                               long backfillEndTimeInMillis, boolean isBackfillEnabled) {
        this.baseEventConnector = baseEventConnector;
        this.streamName = streamName;
        this.streamId = streamId;
        this.sourceType = sourceType;
        this.credentialJson = credentialJson;
        this.backfillStartTimeInMillis = backfillStartTimeInMillis;
        this.backfillEndTimeInMillis = backfillEndTimeInMillis;
        this.isBackfillEnabled  = isBackfillEnabled;
        LOGGER.info("{} Initialized google sheet read util with streamName {}, sourceType {}, " +
                "backfillStartTimeInMillis {}, backfillEndTimeInMillis, isBackfillEnabled", streamId, streamName,
                sourceType, backfillStartTimeInMillis, backfillEndTimeInMillis, isBackfillEnabled);
    }

    public boolean readSheet(Spreadsheet spreadsheet, String spreadSheetId, String trackingColumnName,
                             String trackingColumnPattern)
            throws Exception {

        LOGGER.info("{} Inside read sheet with id {}, trackingColumnName {}, trackingColumnPatter {}",
                streamId, spreadSheetId, trackingColumnName, trackingColumnPattern);
        /**
         * if stream name is all
         * Get sheet names, sort sheet names
         * otherwise we have only 1 sheet
         * For each sheet
         *   get the value of the identifier column
         *   map of identifier column vs list of json record
         *   iterate over that map and process the record
         *   save the state - per sheet
         * */
        boolean publishedEvents = false;
        try {
            Sheets sheetsService = getSheetsService(credentialJson);
            List<Sheet> sheets = spreadsheet.getSheets();
            if (streamName.equals(GoogleSheetConstants.ALL_STREAMS)) {
                //Sort the sheets by name
                sheets.sort(Comparator.comparing(Sheet::getProperties, Comparator.comparing(p -> p.getTitle())));
            }

            for (Sheet sheet : spreadsheet.getSheets()) {
                String sheetName = sheet.getProperties().getTitle();

                if (!streamName.equals(GoogleSheetConstants.ALL_STREAMS) && !streamName.equals(sheetName)) {
                    continue;
                }

                // Fetch all data from the sheet
                ValueRange response = sheetsService.spreadsheets().values()
                        .get(spreadSheetId, sheetName)
                        .execute();
                // Process the data
                List<List<Object>> values = response.getValues();
                Map<Long, List<JsonNode>> trackingColumnValueToRecords =
                        getTrackingColumnValueToRecords(spreadSheetId, sheetName, trackingColumnName,
                                trackingColumnPattern, values);
                publishedEvents = processRecords(spreadSheetId, sheetName, trackingColumnValueToRecords);
            }
        } catch (Exception e) {
            LOGGER.error("{} Unable to read data for preview for stream {}", streamId, e);
            throw e;
        }

        return publishedEvents;
    }

    private boolean processRecords(String sheetId, String sheetName,
                                   Map<Long, List<JsonNode>> trackingColumnValueToRecords) {

        boolean publishedEvents = false;
        List<JsonNode> recordsProcessed = new ArrayList<>();
        int recordsProcessedCounter = 0;
        long maxTimestamp = getCurrentState(sheetId, sheetName);
        long trackingColumnValue = 0L;
        for (Map.Entry<Long, List<JsonNode>> entry : trackingColumnValueToRecords.entrySet()) {
            trackingColumnValue = entry.getKey();

            if (!shouldProcessRawEvent(trackingColumnValue, maxTimestamp)) {
                LOGGER.warn("Ignoring events for timestamp {} either because its less than state or " +
                        "doesn't fall in backfill start time and backfill end time", trackingColumnValue);
                continue;
            }
            List<JsonNode> jsonNodes = entry.getValue();
            recordsProcessed.addAll(entry.getValue());

            if (recordsProcessed.size() >= batchSize) {
                handleJsonEvents(sheetId, sheetName, jsonNodes, trackingColumnValue);
                recordsProcessedCounter += jsonNodes.size() - 1;
                publishedEvents = true;
                recordsProcessed.clear();
            }
        }

        if (recordsProcessed.size() > 0) {
            handleJsonEvents(sheetId, sheetName, recordsProcessed, trackingColumnValue);
            recordsProcessedCounter += recordsProcessed.size() - 1;
        }

        LOGGER.info("Records processed for spread sheet Id {}, sheet name {} are", sheetId, sheetId,
                recordsProcessedCounter);

        return publishedEvents;
    }

    private boolean shouldProcessRawEvent(long eventTimestamp, long maxTimestamp) {

        if (eventTimestamp <= maxTimestamp) {
            return false;
        }
        if (!isBackfillEnabled) {
            return true;
        }
        if (backfillStartTimeInMillis != -1 && eventTimestamp < backfillStartTimeInMillis) {
            return false;
        }
        if (backfillEndTimeInMillis != -1 && eventTimestamp > backfillEndTimeInMillis) {
            return false;
        }

        return true;
    }

    private Long getCurrentState(String spreadSheetId, String sheetName) {
        String key = getStateKey(spreadSheetId, sheetName);
        AirbyteStateMessage state =
                baseEventConnector.getState(baseEventConnector.getAuthInfo(), streamId);
        if (state == null) {
            return -1L;
        }
        JsonNode jsonNode = state.getData();
        if (jsonNode == null) {
            return -1L;
        }
        if (jsonNode.has(key)) {
            return jsonNode.get(key).asLong();
        }

        return -1L;
    }

    private void handleJsonEvents(String spreadSheetId, String sheetName, List<JsonNode> jsonNodes,
                                  long trackingColumnValue) {

        boolean isPublishSuccess = processAndPublishEvents(jsonNodes);
        if (isPublishSuccess) {
            updateState(spreadSheetId, sheetName, trackingColumnValue);
        } else {
            throw new RuntimeException("Unable to publish events, cannot move ahead for stream Id " + streamId);
        }
    }

    private void updateState(String spreadSheetId, String sheetName, long trackingColumnValue) {
        String key = getStateKey(spreadSheetId, sheetName);
        AirbyteStateMessage airbyteStateMessage = baseEventConnector.getState(baseEventConnector.getAuthInfo(),
                streamId);
        JsonNode jsonNode = null;
        if (airbyteStateMessage == null) {
            jsonNode = objectMapper.createObjectNode();
        } else {
            jsonNode = airbyteStateMessage.getData();
        }

        if (jsonNode != null) {
            ObjectNode objectNode = (ObjectNode) jsonNode;
            objectNode.put(key, trackingColumnValue);
        } else {
            jsonNode = objectMapper.createObjectNode();
            ObjectNode objectNode = (ObjectNode) jsonNode;
            objectNode.put(key, trackingColumnValue);
        }

        baseEventConnector.setState(baseEventConnector.getAuthInfo(), streamId, jsonNode);
        LOGGER.info("{} Updating state with {}", streamId, jsonNode);
    }

    private String getStateKey(String spreadSheetId, String sheetName) {
        return spreadSheetId + "_" + sheetName;
    }

    private Map<Long, List<JsonNode>> getTrackingColumnValueToRecords(String sheetId, String sheetName,
                                                                      String trackingColumnName,
                                                                      String trackingColumnPattern,
                                                                      List<List<Object>> data) {

        long savedTrackingColumnValue = getCurrentState(sheetId, sheetName);

        List<Object> headers = data.get(0);
        int trackingColumnIndex = findHeaderIndexByName(headers, trackingColumnName);
        LOGGER.info("{} tracking column index {}", streamId, trackingColumnIndex);
        Map<Long, List<JsonNode>> trackingColumnToJsonRecords = new TreeMap<>();

        // Iterate over rows starting from the second row (index 1)
        for (int i = 1; i < data.size(); i++) {
            List<Object> rowData = data.get(i);
            Map<String, Object> rowMap = new HashMap<>();

            if (rowData.size() < headers.size()) {
                LOGGER.warn("{} Ignoring row for sheet {}", streamId, sheetName);
                continue;
            }
            Long currentTrackingColumnValue = -1L;
            // Iterate over columns and populate the map
            for (int j = 0; j < headers.size(); j++) {
                String columnName = headers.get(j).toString();
                Object columnValue = rowData.get(j);
                rowMap.put(columnName, columnValue);
                if (trackingColumnIndex == j) {
                    currentTrackingColumnValue =
                            getTrackingColumnValue(trackingColumnIndex, trackingColumnPattern, columnValue);
                }
            }
            if (currentTrackingColumnValue == null) {
                LOGGER.warn("{} skipping record as tracking column value is null", streamId);
                continue;
            }
            //This means treat row number as tracking column index
            if (trackingColumnIndex == -1) {
                currentTrackingColumnValue = Long.valueOf(i);
            }

            if (currentTrackingColumnValue <= savedTrackingColumnValue) {
                continue;
            }
            // Convert the map to a JSON node
            JsonNode jsonNode = objectMapper.valueToTree(rowMap);
            if (trackingColumnToJsonRecords.containsKey(currentTrackingColumnValue)) {
                trackingColumnToJsonRecords.get(currentTrackingColumnValue).add(jsonNode);
            } else {
                List<JsonNode> nodes = new ArrayList<>();
                nodes.add(jsonNode);
                trackingColumnToJsonRecords.put(currentTrackingColumnValue, nodes);
            }
        }
        LOGGER.info("{} trackingColumnToRecordsSize {}", streamId, trackingColumnToJsonRecords.size());
        return trackingColumnToJsonRecords;
    }

    private Long getTrackingColumnValue(int trackingColumnIndex, String trackingColumnPattern, Object value) {
        if (trackingColumnIndex == -1) {
            return 0L;
        }
        try {
            return convertStringToTimestamp(value.toString(), trackingColumnPattern);
        } catch (Exception e) {
            LOGGER.error("{} Unable to convert to timestamp for tracking column pattern {} and value {} {}",
                    streamId, trackingColumnPattern, value, e);
            throw e;
        }
    }

    private long convertStringToTimestamp(String dateString, String dateTimePattern) {
        if (dateString == null) {
            return -1;
        }
        if (dateTimePattern.equals(GoogleSheetConstants.READ_TRACKING_COLUMN_FORMAT_EPOCH_MILLIS) ||
                dateTimePattern.equals(GoogleSheetConstants.READ_TRACKING_COLUMN_FORMAT_EPOCH_MICROS)) {
            return Long.parseLong(dateString);
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateTimePattern);
        long milliseconds = 0;
        try {
            // Parse the string into a ZonedDateTime
            ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateString, formatter);
            // Get the milliseconds since the epoch
            milliseconds = zonedDateTime.toInstant().toEpochMilli();
            return milliseconds;
        } catch (Exception e) {
            try {
                LocalDateTime localDateTime = LocalDateTime.parse(dateString, formatter);
                ZoneId z = ZoneId.of("UTC");
                ZonedDateTime zdt = localDateTime.atZone(z);
                milliseconds = zdt.toInstant().getEpochSecond() * 1000;
            } catch (Exception e1) {

                try {
                    formatter = DateTimeFormatter.ofPattern(DATE_TIME_FORMAT_FALLBACK_PATTERN);
                    ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateString, formatter);
                    // Get the milliseconds since the epoch
                    milliseconds = zonedDateTime.toInstant().toEpochMilli();
                } catch (Exception e2) {
                    LOGGER.info("Timestamp unable to parse " + dateString);
                    throw new RuntimeException("Unable to get datetime field value", e2);
                }
            }
        }

        return milliseconds;
    }

    private int findHeaderIndexByName(List<Object> headers, String headerName) {
        for (int i = 0; i < headers.size(); i++) {
            if (headerName.equals(headers.get(i).toString())) {
                return i + 1; // Add 1 to convert from 0-based index to 1-based column number
            }
        }
        return -1; // Return -1 if header not found
    }

    private boolean processAndPublishEvents(List<JsonNode> jsonList) {
        List<RawEvent> rawEvents = new ArrayList<>();
        for (JsonNode json : jsonList) {
            JsonRawEvent jsonRawEvent = baseEventConnector.createJsonRawEvent(json);
            rawEvents.add(jsonRawEvent);
        }

        AuthInfo authInfo = baseEventConnector.getAuthInfo();
        EventSourceInfo eventSourceInfo = new EventSourceInfo(streamId, sourceType);

        EventProcessorResult eventProcessorResult = baseEventConnector.convertRawEventsToBicycleEvents(authInfo,
                eventSourceInfo, rawEvents);

        //TODO: need retry
        boolean publishEvents = true;
        publishEvents = baseEventConnector.publishEvents(authInfo, eventSourceInfo, eventProcessorResult);
        return publishEvents;
    }

    public Sheets getSheetsService(String credentialJson) throws IOException, GeneralSecurityException {
        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

        // Load service account credentials from JSON key file
        GoogleCredential credential = GoogleCredential.fromStream(new ByteArrayInputStream(credentialJson
                        .getBytes(Charset.defaultCharset())))
                .createScoped(Collections.singleton(SheetsScopes.SPREADSHEETS_READONLY));

        // Build Google Sheets service
        return new Sheets.Builder(httpTransport, JSON_FACTORY, credential)
                .setApplicationName(APPLICATION_NAME)
                .build();
    }

    public List<JsonNode> convertSheetDataToJson(String sheetName, List<List<Object>> data, int limit,
                                                 String trackingColumnName, String trackingColumnPattern) {
        LOGGER.info("Sheet: " + sheetName);
        List<JsonNode> jsonNodes = new ArrayList<>();
        int counter = 0;
        boolean applyLimit = limit == -1 ? false : true;
        List<Object> headers = data.get(0);

        int trackingColumnIndex = findHeaderIndexByName(headers, trackingColumnName);
        // Iterate over rows starting from the second row (index 1)
        for (int i = 1; i < data.size(); i++) {
            List<Object> rowData = data.get(i);
            Map<String, Object> rowMap = new HashMap<>();

            if (rowData.size() < headers.size()) {
                continue;
            }
            // Iterate over columns and populate the map
            for (int j = 0; j < headers.size(); j++) {
                String columnName = headers.get(j).toString();
                Object columnValue = rowData.get(j);
                rowMap.put(columnName, columnValue);
                if (trackingColumnIndex == j) {
                    getTrackingColumnValue(trackingColumnIndex, trackingColumnPattern, columnValue);
                }
            }

            // Convert the map to a JSON node
            JsonNode jsonNode = objectMapper.valueToTree(rowMap);
            jsonNodes.add(jsonNode);
            counter++;
            if (applyLimit && counter == limit) {
                break;
            }
        }

        return jsonNodes;
    }
}
