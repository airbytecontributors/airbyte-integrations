package io.bicycle.airbyte.integrations.source.csv;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.inception.server.auth.model.AuthInfo;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
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
import java.util.UUID;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sumitmaheshwari
 * Created on 08/12/2023
 */
public class CSVProdConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(CSVProdConnector.class);
    private static final List<String> BLACKLISTED_DIRS = new ArrayList<>();
    private static final int CONNECT_TIMEOUT_IN_MILLIS = 60000;
    private static final int READ_TIMEOUT_IN_MILLIS = 60000;
    private static final String LAST_UPDATED_TIMESTAMP = "lastUpdatedTimestampInEpochMillis";
    private static final String DATE_TIME_FORMAT_FALLBACK_PATTERN = "yyyy-MM-dd HH:mm:ss z";
    private static final String CSV_FILE_TYPE = "csv";
    private static final String ZIP_FILE_TYPE = "zip";
    private static final String OUTPUT_DIRECTORY = "/tmp/csvfiles";
    private final String fileUrl;
    private final String dateTimePattern;
    private final String dateTimeFieldColumnName;
    private final int batchSize;
    private final long delay;
    private final String backfillJobId;
    private final CSVConnector csvConnector;
    private final String streamId;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JsonNode config;
    private final String sourceType;
    private final String timeZone;
    private final int dummyMessageInterval;
    private final boolean isBackfillEnabled;
    private long backfillStartTimeInMillis = -1;
    private long backfillEndTimeInMillis = -1;

    public CSVProdConnector(String fileUrl, String dateTimePattern, String timeZone, String dateTimeFieldColumnName,
                            String backfillJobId, String streamId, String sourceType, CSVConnector csvConnector,
                            int batchSize, long delay, JsonNode config) {
        this.streamId = streamId;
        this.sourceType = sourceType;
        this.fileUrl = fileUrl;
        this.dateTimePattern = dateTimePattern;
        this.dateTimeFieldColumnName = dateTimeFieldColumnName;
        this.backfillJobId = backfillJobId;
        this.batchSize = batchSize;
        this.delay = delay;
        this.csvConnector = csvConnector;
        this.config = config;
        this.timeZone = timeZone;
        this.dummyMessageInterval = config.has("dummyMessageInterval")
                ? config.get("dummyMessageInterval").asInt() : 120;
        isBackfillEnabled = config.get("backfill") != null ? config.get("backfill").asBoolean() : false;
        String backfillStartTimestamp
                = config.get("backfillStartDateTime") != null ? config.get("backfillStartDateTime").asText() : null;
        this.backfillStartTimeInMillis = convertStringToTimestamp(backfillStartTimestamp);

        String backfillEndTimestamp
                = config.get("backfillEndDateTime") != null ? config.get("backfillEndDateTime").asText() : null;
        this.backfillEndTimeInMillis = convertStringToTimestamp(backfillEndTimestamp);

        BLACKLISTED_DIRS.add("MACOSX");
    }


    public void doRead() {

        LOGGER.info("Inside do read for connector {}", streamId);

        String fileType = getFileType(fileUrl);
        LOGGER.info("File type identified for connector {} is {}", streamId, fileType);

        if (StringUtils.isEmpty(fileType)) {
            throw new RuntimeException("Unable to determine the file type for fileurl " + fileUrl);
        }

        File[] files;
        File file = getFileObject(fileType);
        LOGGER.info("Able to get file object for stream Id {} {}", streamId, file.getPath());

        try {
            if (fileType.equals(ZIP_FILE_TYPE)) {
                files = handleZipFile(file.getPath());
            } else {
                files = new File[1];
                files[0] = file;
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to read file from url " + fileUrl, e);
        }

        LOGGER.info("Got {} files in the location for stream Id {}", files.length, streamId);

        for (int i = 0; i < files.length; i++) {
            processCSVFile(files[i]);
        }

        EventSourceInfo eventSourceInfo = new EventSourceInfo(streamId, sourceType);
        if (isBackfillEnabled) {
            LOGGER.info("Starting publishing dummy events");
            publishDummyEvents(eventSourceInfo, dummyMessageInterval);
            LOGGER.info("Done publishing dummy events");
        }
    }

    private String getFileType(String fileUrl) {
        //it could be a signed url or it could be url with bucket service account json
        final JsonNode provider = config.get("provider");
        String fileExtension = null;
        if (provider.get("storage").asText().equals("GCS")) {
            fileExtension = getFileExtension(fileUrl);
        } else {
            fileExtension = getFileExtensionFromPublicUrl();
        }

        return fileExtension;
    }

    private String getFileExtensionFromPublicUrl() {

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(fileUrl);

            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                String contentType = response.getFirstHeader("Content-Type").getValue();
                if (StringUtils.isEmpty(contentType)) {
                    return CSV_FILE_TYPE;
                }
                if (contentType.toLowerCase().contains(CSV_FILE_TYPE)) {
                    return CSV_FILE_TYPE;
                } else if (contentType.toLowerCase().contains(ZIP_FILE_TYPE)) {
                    return ZIP_FILE_TYPE;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to find the file type", e);
        }

        return CSV_FILE_TYPE;
    }

    private File getFileObject(String fileType) {

        try {
            File file = File.createTempFile(UUID.randomUUID().toString(), "." + fileType);
            file.deleteOnExit();
            final JsonNode provider = config.get("provider");

            if (provider.get("storage").asText().equals("GCS")) {
                csvConnector.storeToFile(config, file);
            } else {
                FileUtils.copyURLToFile(new URL(fileUrl), file, CONNECT_TIMEOUT_IN_MILLIS, READ_TIMEOUT_IN_MILLIS);
            }
            return file;
        } catch (Exception e) {
            throw new RuntimeException("Unable to read file from GCS", e);
        }
    }

    private String getFileExtension(String filePath) {
        Path path = Paths.get(filePath);
        String fileName = path.getFileName().toString();

        int dotIndex = fileName.lastIndexOf('.');
        if (dotIndex > 0 && dotIndex < fileName.length() - 1) {
            return fileName.substring(dotIndex + 1).toLowerCase();
        } else {
            return null; // No file extension found
        }
    }

    private File[] handleZipFile(String filePath) {

        try {
            List<File> files = getCSVFilesFromZip(filePath);
            File[] filesArray = files.toArray(new File[files.size()]);
            return filesArray;
        } catch (Exception e) {
            throw new RuntimeException("Unable to read zip file", e);
        }
    }



    public List<File> getCSVFilesFromZip(String zipFileAbsPath) throws IOException {
        List<File> csvFiles = new ArrayList<>();
        Path zipFilePath = Paths.get(zipFileAbsPath);
        FileSystem zipFileSystem = FileSystems.newFileSystem(zipFilePath);
        Path root = zipFileSystem.getPath("/");
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                if (isFileAllowed(path.toString())) {

                    File file = new File(OUTPUT_DIRECTORY, path.getFileName().toString());
                    file.deleteOnExit();
                    if (file.getParentFile() != null) {
                        file.getParentFile().mkdirs();
                    }

                    Path outputPath = Paths.get(OUTPUT_DIRECTORY, path.getFileName().toString());
                    Files.copy(Files.newInputStream(path), outputPath, StandardCopyOption.REPLACE_EXISTING);
                    csvFiles.add(file);
                    System.out.println(path);
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return csvFiles;
    }

    private boolean isFileAllowed(String filePath) {

        for (String fileName : BLACKLISTED_DIRS) {
            if (filePath.contains(fileName)) {
                return false;
            }
        }

        if (filePath.endsWith(".csv")) {
            return true;
        }

        return false;
    }

    private void processCSVFile(File csvFile) {

        try {
            long maxTimestamp = getState();

            List<String[]> csvData = readCsvFile(csvFile.getPath());

            //Sort the data in csv
            csvData = sortByColumn(csvData, getHeaderIndex(csvFile.getPath(), dateTimeFieldColumnName));

            LOGGER.info("CSV FileName:: {}", csvFile.getName());
            LOGGER.info("CSV rows:: {}", csvData.size());

            //convert csv rows to json rows
            List<String> jsonList = convertCsvToJson(csvData, maxTimestamp);
            LOGGER.info("Json rows:: {}", jsonList.size());
            long maxTimestampPublished = handleRawEvents(jsonList, maxTimestamp);
            LOGGER.info("MaxTimeStamp published:: {}", maxTimestampPublished);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private long handleRawEvents(List<String> jsonList, long maxTimestamp) {

        for (int i = 0; i < jsonList.size(); i += batchSize) {
            List<String> batch = jsonList.subList(i, Math.min(i + batchSize, jsonList.size()));
            List<JsonNode> jsonNodes = convertJsonStringListToJsonNodeList(batch);
            maxTimestamp = getMaxTimestamp(jsonNodes, maxTimestamp);

            boolean isPublishSuccess = processAndPublishEvents(jsonNodes);

            if (isPublishSuccess) {
                setState(maxTimestamp);
            } else {
              throw new RuntimeException("Unable to publish events, cannot move ahead for stream Id " + streamId);
            }

            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        return maxTimestamp;
    }

    private List<JsonNode> convertJsonStringListToJsonNodeList(List<String> jsonStrings) {
        List<JsonNode> jsonNodes = new ArrayList<>();
        for (String str : jsonStrings) {
            try {
                JsonNode jsonNode = objectMapper.readTree(str);
                jsonNodes.add(jsonNode);
            } catch (Exception e) {
                LOGGER.error("Unable to convert json string {} to json node because of {}", str, e);
            }

        }

        return jsonNodes;
    }

    private long getMaxTimestamp(List<JsonNode> jsonEvents, long maxTimestamp) {

        for (JsonNode jsonNode : jsonEvents) {
            long timestamp = getTimestampFieldValue(jsonNode);
            if (timestamp > maxTimestamp) {
                maxTimestamp = timestamp;
            }
        }

        return maxTimestamp;
    }

    private long getTimestampFieldValue(JsonNode jsonNode) {

        if (jsonNode.has(dateTimeFieldColumnName)) {
            String timestampValue = jsonNode.get(dateTimeFieldColumnName).asText();
            return convertStringToTimestamp(timestampValue);
        }
        return 0;
    }

    private boolean processAndPublishEvents(List<JsonNode> jsonList) {
        List<RawEvent> rawEvents = new ArrayList<>();
        for (JsonNode json : jsonList) {
            JsonRawEvent jsonRawEvent = csvConnector.createJsonRawEvent(json);
            rawEvents.add(jsonRawEvent);
        }

        AuthInfo authInfo = csvConnector.getAuthInfo();
        EventSourceInfo eventSourceInfo = new EventSourceInfo(streamId, sourceType);

        EventProcessorResult eventProcessorResult = csvConnector.convertRawEventsToBicycleEvents(authInfo,
                eventSourceInfo, rawEvents);

        //TODO: need retry
        boolean publishEvents = true;
        publishEvents = csvConnector.publishEvents(authInfo, eventSourceInfo, eventProcessorResult);
        return publishEvents;
    }

    private static List<String[]> readCsvFile(String csvFilePath) throws IOException, CsvValidationException {
        List<String[]> csvData = new ArrayList<>();

        try (CSVReader csvReader = new CSVReader(new FileReader(csvFilePath))) {
            String[] nextRecord;
            while ((nextRecord = csvReader.readNext()) != null) {
                csvData.add(nextRecord);
            }
        }

        return csvData;
    }


    private List<String[]> sortByColumn(List<String[]> data, final int columnIndex) {
        List<String[]> dataCopy = new ArrayList<>();
        dataCopy.add(data.get(0));
        //first row has header names so removed that
        data.remove(0);
        Collections.sort(data, Comparator.comparing(row -> convertStringToTimestamp(row[columnIndex])));
        dataCopy.addAll(data);
        return dataCopy;
    }

    private long convertStringToTimestamp(String dateString) {
        if (dateString == null) {
            return -1;
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
                ZoneId z = ZoneId.of(timeZone);
                ZonedDateTime zdt = localDateTime.atZone(z);
                milliseconds = zdt.toInstant().getEpochSecond() * 1000;
            } catch (Exception e1) {

                try {
                    formatter = DateTimeFormatter.ofPattern(DATE_TIME_FORMAT_FALLBACK_PATTERN);
                    ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateString, formatter);
                    // Get the milliseconds since the epoch
                    milliseconds = zonedDateTime.toInstant().toEpochMilli();
                } catch (Exception e2) {
                    System.out.println("Timestamp unable to parse " + dateString);
                    throw new RuntimeException("Unable to get datetime field value", e2);
                }
            }
        }

        return milliseconds;
    }

    private List<String> convertCsvToJson(List<String[]> csvData, long timestamp) {
        List<String> jsonList = new ArrayList<>();

        int i = -1;
        for (String[] csvRow : csvData) {
            try {
                // Assuming the first row of CSV contains headers
                i++;
                if (i == 0) {
                    continue;
                }
                String json = convertRowToJson(csvData.get(0), csvRow, timestamp);
                if (!StringUtils.isEmpty(json)) {
                    jsonList.add(json);
                }
            } catch (Exception e) {
                throw new RuntimeException("Unable to convert one of the csv row " + csvRow, e);
            }
        }

        return jsonList;
    }

    private String convertRowToJson(String[] headers, String[] values, long maxTimestampProcessed) {
        Map<String, String> jsonMap = new HashMap<>();

        for (int i = 0; i < headers.length; i++) {
            String headerName = headers[i];
            headerName = headerName.replaceAll("\\.", "_");
            jsonMap.put(headerName, values[i]);
        }

        try {
            String timestampString = jsonMap.get(dateTimeFieldColumnName);
            long timestamp = convertStringToTimestamp(timestampString);
            if (timestamp <= maxTimestampProcessed) {
                return null;
            }
            if (backfillStartTimeInMillis != -1 && timestamp < backfillEndTimeInMillis) {
                return null;
            }
            if (backfillEndTimeInMillis != -1 && timestamp > backfillEndTimeInMillis) {
                return null;
            }
            return objectMapper.writeValueAsString(jsonMap);
        } catch (Exception e) {
            LOGGER.error("Unable to convert a csv row {} to json because of {}", values, e);
            return null; // Return an empty JSON object in case of an error
        }
    }

    public int getHeaderIndex(String csvFilePath, String headerName) throws IOException {
        CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader().withIgnoreHeaderCase();

        try (CSVParser csvParser = new CSVParser(new FileReader(csvFilePath), csvFormat)) {
            int index = csvParser.getHeaderMap().get(headerName);
            return index;
        }
    }

    private long getState() {
        AirbyteStateMessage airbyteStateMessage = csvConnector.getState(this.csvConnector.getAuthInfo(), streamId);
        if (airbyteStateMessage == null) {
            return 0;
        }
        JsonNode jsonNode = airbyteStateMessage.getData();
        long lastUpdatedTimestamp = 0;
        if (jsonNode.has(LAST_UPDATED_TIMESTAMP)) {
            lastUpdatedTimestamp = jsonNode.get(LAST_UPDATED_TIMESTAMP).asLong();
        }

        return lastUpdatedTimestamp;
    }

    private void setState(long timestamp) {
        JsonNode state = this.csvConnector.getUpdatedState(LAST_UPDATED_TIMESTAMP, timestamp);
        boolean isStateSaved = csvConnector.setState(this.csvConnector.getAuthInfo(), streamId, state);
        if (isStateSaved) {
            LOGGER.info("Successfully saved state for stream Id {}", streamId);
        } else {
            LOGGER.warn("Unable to save state for stream Id {}", streamId);
        }
    }

    private void publishDummyEvents(EventSourceInfo eventSourceInfo, long dummyMessageInterval) {
        csvConnector.publishDummyEvents(csvConnector.getAuthInfo(), eventSourceInfo, dummyMessageInterval);
    }


}
