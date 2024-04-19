package io.airbyte.integrations.bicycle.base.integration.reader.csv;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.integrations.bicycle.base.integration.BaseCSVEventConnector;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.exception.UnsupportedFormatException;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;

public class CSVEventSourceReaderV2 extends CSVEventSourceReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVEventSourceReaderV2.class);

    private CSVParser csvParser;
    private Iterator<CSVRecord> iterator;

    public CSVEventSourceReaderV2(String name, File csvFile, String connectorId,
                                  BaseEventConnector connector, BaseCSVEventConnector.APITYPE apiType) {
        super(name, csvFile, connectorId, connector, apiType);
    }

    protected void initialize() {
        try {
            csvParser = CSVParser.parse(new FileReader(csvFile, Charset.defaultCharset()),
                                                                CSVFormat.DEFAULT.withFirstRecordAsHeader());
            headerNameToIndexMap = csvParser.getHeaderMap();
            if (headerNameToIndexMap.isEmpty()) {
                throw new RuntimeException("Unable to read headers from csv file " + csvFile + " for " + connectorId);
            }
            iterator = csvParser.iterator();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read csv file ["+csvFile+"]", e);
        }
    }

    public void validateFileFormat() throws UnsupportedFormatException {
        int count = 0;
        try {
            while (hasNext() && count < 10) {
                RawEvent nextEvent = next();
                count++;
            }
        } catch (Exception e) {
            throw new UnsupportedFormatException(csvFile.getName() + " - line number ["+rowCounter+"] ["+count+"]");
        }
    }

    public void seek(long offset, long rowCounter) throws IOException {
        Iterator<CSVRecord> itr = csvParser.iterator();
        while(itr.hasNext()) {
            CSVRecord next = itr.next();
            long recordNumber = next.getRecordNumber();
            if (recordNumber == offset) {
                this.csvRecord = next;
                this.rowCounter = csvParser.getCurrentLineNumber();
                this.offset = csvParser.getRecordNumber();
                this.row = csvRecord.toString();
                break;
            }
        }
    }

    public boolean hasNext() {
        boolean b = iterator.hasNext();
        if (b) {
            csvRecord = iterator.next();
            rowCounter = csvParser.getCurrentLineNumber();
            offset = csvParser.getRecordNumber();
            row = csvRecord.toString();
        }
        return b;
    }

    public RawEvent next() {
        String errorMessage = null;
        try {
            if (csvRecord != null) {
                nextEvent = convertRecordsToRawEvents(headerNameToIndexMap, csvRecord, rowCounter, name);
                return nextEvent;
            }
        } catch (Exception e) {
            validEvent = false;
            errorMessage = "[" + e.getMessage() + "] [" + rowCounter + "] [" + row + "]";
            LOGGER.error("Failed Parsing [" + row + "] [" + rowCounter + "]", e);
        } finally {
            if (!isValidEvent()) {
                status = BaseEventConnector.ReaderStatus.FAILED;
            }
        }
        ObjectNode node = mapper.createObjectNode();
        node.put("bicycle.raw.event.record", row);
        nextEvent = getJsonRawEvent(rowCounter, name, node, errorMessage);
        return nextEvent;
    }

    public void close() throws Exception {
        if (csvParser != null && !csvParser.isClosed()) {
            csvParser.close();
        }
    }

}
