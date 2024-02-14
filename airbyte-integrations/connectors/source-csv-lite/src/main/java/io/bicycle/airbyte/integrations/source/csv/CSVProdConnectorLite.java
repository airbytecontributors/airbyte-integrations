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
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.net.URL;
import java.nio.charset.Charset;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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
public class CSVProdConnectorLite {
    private static final Logger LOGGER = LoggerFactory.getLogger(CSVProdConnectorLite.class);
    private static final List<String> BLACKLISTED_DIRS = new ArrayList<>();
    private static final int CONNECT_TIMEOUT_IN_MILLIS = 60000;
    private static final int READ_TIMEOUT_IN_MILLIS = 60000;
    private static final String LAST_UPDATED_TIMESTAMP = "lastUpdatedTimestampInEpochMillis";
    private static final String DATE_TIME_FORMAT_FALLBACK_PATTERN = "yyyy-MM-dd HH:mm:ss z";
    private static final String CSV_FILE_TYPE = "csv";
    private static final String ZIP_FILE_TYPE = "zip";
    private static final String OUTPUT_DIRECTORY = "/tmp/csvfiles";
    private String fileUrl;
    private String dateTimePattern;
    private String dateTimeFieldColumnName;
    private int batchSize;
    private long delay;
    private String backfillJobId;
    private CSVConnectorLite csvConnector;
    private String streamId;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private JsonNode config;
    private String sourceType;
    private String timeZone;
    private int dummyMessageInterval;
    private boolean isBackfillEnabled;
    private long backfillStartTimeInMillis = -1;
    private long backfillEndTimeInMillis = -1;

    //if we do in multiple threads will need to have thread local map
    private Map<String, Integer> headerNameToIndexMap;

    public CSVProdConnectorLite(String fileUrl, String dateTimePattern, String timeZone, String dateTimeFieldColumnName,
                            String backfillJobId, String streamId, String sourceType, CSVConnectorLite csvConnector,
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

    public CSVProdConnectorLite(String fileUrl, JsonNode config, CSVConnectorLite csvConnector) {
        this.fileUrl = fileUrl;
        this.streamId = "unknown";
        this.sourceType = "unknown";
        this.config = config;
        this.csvConnector = csvConnector;
    }
    public void doRead() throws IOException {

        File[] files = getFilesObject();
        EventSourceInfo eventSourceInfo = new EventSourceInfo(streamId, sourceType);
        boolean doesMappingRulesExist = csvConnector.doesMappingRulesExists(csvConnector.getAuthInfo(),
                eventSourceInfo);

        for (int i = 0; i < files.length; i++) {
            processCSVFile(files[i], doesMappingRulesExist);
        }

        if (isBackfillEnabled) {
            LOGGER.info("Starting publishing dummy events for stream Id {}", streamId);
            publishDummyEvents(eventSourceInfo, dummyMessageInterval);
            LOGGER.info("Done publishing dummy events for stream Id {}", streamId);
        }

        csvConnector.stopEventConnector();
    }

    public File[] getFilesObject() {
        LOGGER.info("Inside do read for connector {}", streamId);

        String fileType = getFileType(fileUrl);
        LOGGER.info("File type identified for connector {} is {}", streamId, fileType);

        if (StringUtils.isEmpty(fileType)) {
            throw new RuntimeException("Unable to determine the file type for fileurl " + fileUrl +
                    " and stream Id " + streamId);
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
            throw new RuntimeException("Unable to read file from url " + fileUrl + " for stream Id " + streamId, e);
        }

        LOGGER.info("Got {} files in the location for stream Id {}", files.length, streamId);

        return files;
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
            Arrays.sort(filesArray, Comparator.comparing(File::getName));
            return filesArray;
        } catch (Exception e) {
            throw new RuntimeException("Unable to read zip file", e);
        }
    }

    private List<File> getCSVFilesFromZip(String zipFileAbsPath) throws IOException {
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

    private void processCSVFile(File csvFile, boolean doesMappingRuleExists) throws IOException {

        RandomAccessFile accessFile = null;

        try {
            long maxTimestamp = getState();
            LOGGER.info("Current state for connector with id {} is {} and processing file {}", streamId, maxTimestamp,
                    csvFile.getName());

            Map<Long, List<Long>> timeStampToOffset = getTimestampToFileOffset(csvFile.getPath());
            List<String[]> csvRecords = new ArrayList<>();
            accessFile = new RandomAccessFile(csvFile.getPath(), "r");
            boolean headerAdded = false;
            long recordsProcessed = 0;
            for (Map.Entry<Long, List<Long>> entry: timeStampToOffset.entrySet()) {
                long timestamp = entry.getKey();
                if (!shouldProcessRawEvent(timestamp, maxTimestamp)) {
                    LOGGER.warn("Ignoring events for timestamp {} either because its less than state or " +
                            "doesn't fall in backfill start time and backfill end time", timestamp);
                    continue;
                }
                List<Long> offsets = entry.getValue();
                if (!headerAdded) {
                    csvRecords.add(headerNameToIndexMap.keySet().toArray(new String[headerNameToIndexMap.size()]));
                    headerAdded = true;
                }
                for (Long offset: offsets) {
                    accessFile.seek(offset);
                    String row = accessFile.readLine();
                    CSVRecord record = getCsvRecord(offset, row, headerNameToIndexMap);
                    String[] recordArray = new String[record.size()];
                    Iterator<String> iterator = record.iterator();
                    int i=0;
                    while (iterator.hasNext()) {
                        recordArray[i] = iterator.next();
                        i++;
                    }
                    csvRecords.add(recordArray);
                }

                if (csvRecords.size() >= batchSize) {
                    maxTimestamp = handleCSVRecords(csvRecords, csvFile.getName(), maxTimestamp, doesMappingRuleExists);
                    recordsProcessed += csvRecords.size() - 1;
                    headerAdded = false;
                    csvRecords.clear();
                }
            }

            if (csvRecords.size() > 0) {
                maxTimestamp = handleCSVRecords(csvRecords, csvFile.getName(), maxTimestamp, doesMappingRuleExists);
                recordsProcessed += csvRecords.size() - 1;
                csvRecords.clear();
            }

            LOGGER.info("Total records processed for stream {} and file name {} is {} with max timestamp {}", streamId,
                    csvFile.getPath(), recordsProcessed, maxTimestamp);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }  finally {
            if (accessFile != null) {
                accessFile.close();
            }
        }
    }

    private boolean shouldProcessRawEvent(long eventTimestamp, long maxTimestamp) {

        if (eventTimestamp <= maxTimestamp) {
            return false;
        }
        if (backfillStartTimeInMillis != -1 && eventTimestamp < backfillStartTimeInMillis) {
            return false;
        }
        if (backfillEndTimeInMillis != -1 && eventTimestamp > backfillEndTimeInMillis) {
            return false;
        }

        return true;
    }

    private long handleCSVRecords(List<String[]> csvData, String fileName, long maxTimestamp, boolean doesMappingRuleExists) {
        List<String> jsonList = convertCsvToJson(csvData, maxTimestamp);
        LOGGER.info("Json rows for stream ID {} :: {} with file name {}", streamId, jsonList.size(),
                fileName);
        long maxTimestampPublished = handleRawJsonEvents(jsonList, maxTimestamp, doesMappingRuleExists);
        LOGGER.info("MaxTimeStamp published for connector Id {} :: {} after processing file {}", streamId,
                maxTimestampPublished, fileName);

        return maxTimestampPublished;
    }

    private long handleRawJsonEvents(List<String> jsonList, long maxTimestamp, boolean doesMappingRuleExists) {

        for (int i = 0; i < jsonList.size(); i += batchSize) {
            List<String> batch = jsonList.subList(i, Math.min(i + batchSize, jsonList.size()));
            List<JsonNode> jsonNodes = convertJsonStringListToJsonNodeList(batch);
            maxTimestamp = getMaxTimestamp(jsonNodes, maxTimestamp);

            boolean isPublishSuccess = processAndPublishEvents(jsonNodes);

            if (isPublishSuccess) {
                if (doesMappingRuleExists) {
                    updateState(maxTimestamp);
                }
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

    private Map<Long, List<Long>> getTimestampToFileOffset(String csvFilePath) throws IOException {
        Map<Long, List<Long>> timestampToFileOffsetsMap = new TreeMap<>();
        RandomAccessFile accessFile = null;
        String row = null;
        long counter = 0;
        long nullRows = 0;
        try {
            accessFile = new RandomAccessFile(csvFilePath, "r");
            String headersLine = accessFile.readLine();
//            bytesRead += headersLine.getBytes().length;
            headerNameToIndexMap = getHeaderNameToIndexMap(headersLine);
            if (headerNameToIndexMap.isEmpty()) {
                throw new RuntimeException("Unable to read headers from csv file " + csvFilePath + " for " + streamId);
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
                        timestampToFileOffsetsMap.computeIfAbsent(timestampInMillisInEpoch,
                                (recordOffset) -> new ArrayList<>()).add(offset);
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
        LOGGER.info("Total rows processed for file {} with stream Id {} is {}", csvFilePath, streamId, counter);
        return timestampToFileOffsetsMap;
    }

    private CSVRecord getCsvRecord(long offset, String row, Map<String, Integer> headerNameToIndexMap) {
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

        try {
            for (int i = 0; i < headers.length; i++) {
                String headerName = headers[i];
                headerName = headerName.replaceAll("\\.", "_");
                int index = headerNameToIndexMap.get(headerName);
                jsonMap.put(headerName, values[index]);
            }
        } catch (Exception e) {
            LOGGER.error("Unable to convert a row to json", e);
            return null;
        }


        try {
            String timestampString = jsonMap.get(dateTimeFieldColumnName);
            long timestamp = convertStringToTimestamp(timestampString);
            if (timestamp <= maxTimestampProcessed) {
                return null;
            }
            if (backfillStartTimeInMillis != -1 && timestamp < backfillStartTimeInMillis) {
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

        try (CSVParser csvParser = new CSVParser(new FileReader(csvFilePath, Charset.defaultCharset()), csvFormat)) {
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
        if (jsonNode!= null && jsonNode.has(LAST_UPDATED_TIMESTAMP)) {
            lastUpdatedTimestamp = jsonNode.get(LAST_UPDATED_TIMESTAMP).asLong();
        }

        return lastUpdatedTimestamp;
    }

    private void updateState(long timestamp) {
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

    private static List<String[]> readCsvFile(String csvFilePath) throws IOException, CsvValidationException {
        List<String[]> csvData = new ArrayList<>();

        try (CSVReader csvReader = new CSVReader(new FileReader(csvFilePath, Charset.defaultCharset()))) {
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


}
