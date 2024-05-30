package io.airbyte.integrations.bicycle.base.integration.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.integrations.bicycle.base.integration.BaseCSVEventConnector;
import io.airbyte.integrations.bicycle.base.integration.BaseEventConnector;
import io.airbyte.integrations.bicycle.base.integration.exception.UnsupportedFormatException;
import io.bicycle.entity.mapping.SourceFieldMapping;
import io.bicycle.event.rawevent.impl.JsonRawEvent;
import io.bicycle.server.event.mapping.UserServiceFieldDef;
import io.bicycle.server.event.mapping.UserServiceFieldsList;
import io.bicycle.server.event.mapping.UserServiceFieldsRule;
import io.bicycle.server.event.mapping.UserServiceMappingRule;
import io.bicycle.server.event.mapping.api.MetadataPreviewEventType;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static io.airbyte.integrations.bicycle.base.integration.BaseCSVEventConnector.APITYPE.READ;

public abstract class EventSourceReader<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventSourceReader.class);

    protected ObjectMapper mapper = new ObjectMapper();

    protected BaseEventConnector.ReaderStatus status = BaseEventConnector.ReaderStatus.SUCCESS;

    protected String fileName;
    protected String connectorId;

    protected BaseEventConnector connector;
    protected BaseCSVEventConnector.APITYPE apiType;
    protected SourceFieldMapping fieldMapping;
    protected boolean validEvent = false;
    protected long offset = -1;
    protected long rowCounter = 0;
    protected String row = null;


    public EventSourceReader(String fileName, String connectorId, BaseEventConnector connector,
                             BaseCSVEventConnector.APITYPE apiType) {
        this.fileName = fileName;
        this.connectorId = connectorId;
        this.connector = connector;
        this.apiType = apiType;
    }

    public abstract boolean hasNext();

    public boolean isValidEvent() {
        return validEvent;
    }

    protected BufferedReader getFileReader(String fileName, URL url) throws IOException {
        BufferedReader fileReader;
        if (fileName.endsWith(".gz")) {
            fileReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(url.openStream()), Charset.defaultCharset()), 65536);
        } else {
            fileReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(url.openStream()), Charset.defaultCharset()), 65536);
        }
        return fileReader;
    }

    public long getRecordUTCTimestampInMillis(RawEvent event) {
        SourceFieldMapping fieldMapping = getSourceFieldMapping();
        long valueInMicros = (long) event.getFieldValue(fieldMapping, Collections.emptyMap(), connector.getAuthInfo());
        return valueInMicros / 1000;
    }

    public abstract T next();

    public abstract void close() throws Exception;

    public void seek(long offset, long rowCounter) throws Exception {
        throw new IllegalStateException("Method not implemented");
    }

    protected JsonRawEvent getJsonRawEvent(long rowCounter, String fileName, ObjectNode node, String errorMessage) {
        node.put("bicycle.raw.event.identifier", String.valueOf(rowCounter));
        if (!validEvent) {
            if (apiType.equals(BaseCSVEventConnector.APITYPE.SYNC_DATA)) {
                node.put("bicycle.metadata.eventType", MetadataPreviewEventType.SYNC_ERROR.name());
            } else if (apiType.equals(READ)) {
                node.put("bicycle.metadata.eventType", MetadataPreviewEventType.READ_ERROR.name());
            }
        }
        node.put("bicycle.eventSourceId", connectorId);
        if (errorMessage != null) {
            node.put("bicycle.raw.event.error", errorMessage);
        }
        node.put("bicycle.filename", fileName);
        JsonRawEvent jsonRawEvent = connector.createJsonRawEvent(node);
        return jsonRawEvent;
    }

    protected SourceFieldMapping getSourceFieldMapping() {
        if (fieldMapping != null) {
            return fieldMapping;
        } else {
            UserServiceFieldDef startTimeFieldDef = null;
            List<UserServiceMappingRule> userServiceMappingRules =
                    connector.getUserServiceMappingRules(connector.getAuthInfo(), connector.getEventSourceInfo());
            LOGGER.info("[{}] : Userservice rules downloaded [{}]", connectorId, userServiceMappingRules);
            boolean found = false;
            for (UserServiceMappingRule userServiceMappingRule : userServiceMappingRules) {
                UserServiceFieldsRule userServiceFields = userServiceMappingRule.getUserServiceFields();
                List<UserServiceFieldDef> commonFieldsList = userServiceFields.getCommonFieldsList();
                for (UserServiceFieldDef userServiceFieldDef : commonFieldsList) {
                    if (userServiceFieldDef.getPredefinedFieldType().equals("startTimeMicros")) {
                        startTimeFieldDef = userServiceFieldDef;
                        found = true;
                        break;
                    }
                }
                if (found) {
                    break;
                }

                Map<String, UserServiceFieldsList> userServiceFieldsMap = userServiceFields.getUserServiceFieldsMap();
                for (String key : userServiceFieldsMap.keySet()) {
                    UserServiceFieldsList userServiceFieldsList = userServiceFieldsMap.get(key);
                    for (UserServiceFieldDef userServiceFieldDef : userServiceFieldsList.getUserServiceFieldList()) {
                        if (userServiceFieldDef.getPredefinedFieldType().equals("startTimeMicros")) {
                            startTimeFieldDef = userServiceFieldDef;
                            found = true;
                            break;
                        }
                    }
                    if (found) {
                        break;
                    }
                }
                if (found) {
                    break;
                }
            }
            if (startTimeFieldDef == null) {
                throw new IllegalStateException("timestamp field is not discovered yet");
            }

            fieldMapping = startTimeFieldDef.getFieldMapping();
            return fieldMapping;
        }
    }

    public BaseEventConnector.ReaderStatus getStatus() {
        return status;
    }

    public long getOffset() {
        return offset;
    }

    public long getRowCounter() {
        return rowCounter;
    }

    public String getRow() {
        return row;
    }

    public void validateFileFormat() throws UnsupportedFormatException {
        int count = 0;
        try {
            while (hasNext() && count < 10) {
                T nextEvent = next();
                count++;
            }
        } catch (Exception e) {
            throw new UnsupportedFormatException(fileName + " - line number ["+rowCounter+"] ["+count+"]");
        }
    }
}
