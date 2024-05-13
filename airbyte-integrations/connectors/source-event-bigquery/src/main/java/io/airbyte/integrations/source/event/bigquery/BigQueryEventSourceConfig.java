package io.airbyte.integrations.source.event.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.source.event.bigquery.data.formatter.DataFormatter;
import io.airbyte.integrations.source.event.bigquery.data.formatter.DataFormatterConfig;
import io.airbyte.integrations.source.event.bigquery.data.formatter.DataFormatterFactory;
import io.airbyte.integrations.source.event.bigquery.data.formatter.DataFormatterType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.airbyte.integrations.source.event.bigquery.BicycleBigQueryWrapper.CURSOR_FIELD;

/**
 * @author sumitmaheshwari
 * Created on 13/10/2023
 */
public class BigQueryEventSourceConfig {

    private static final Logger logger = LoggerFactory.getLogger(BigQueryEventSourceConfig.class.getName());
    private final String projectId;
    private final String datasetId;
    private final String credentialsJson;
    private final String defaultCursorValue;
    private final int defaultLimit;
    private String cursorField;
    private DataFormatter dataFormatter;

    public BigQueryEventSourceConfig(JsonNode config, String cursorField) {
        this.projectId = config.has(BicycleBigQueryWrapper.CONFIG_PROJECT_ID) ?
                config.get(BicycleBigQueryWrapper.CONFIG_PROJECT_ID).asText() : null;
        this.datasetId = config.has(BicycleBigQueryWrapper.CONFIG_DATASET_ID) ?
                config.get(BicycleBigQueryWrapper.CONFIG_DATASET_ID).asText() : null;
        this.credentialsJson = config.has(BicycleBigQueryWrapper.CONFIG_CREDS) ?
                config.get(BicycleBigQueryWrapper.CONFIG_CREDS).asText() : null;
        this.defaultCursorValue = config.has("cursor_default_value") ?
                config.get("cursor_default_value").asText() : null;
        this.defaultLimit = config.has("fetch_rows_limit") ?
                config.get("fetch_rows_limit").asInt() : 1000;

        JsonNode syncMode = config.has(BicycleBigQueryWrapper.SYNC_MODE)
                ? config.get(BicycleBigQueryWrapper.SYNC_MODE) : null;

        if (syncMode != null) {
            String syncType = syncMode.get(BicycleBigQueryWrapper.SYNC_TYPE).asText();
            if (StringUtils.isNotEmpty(syncType) && syncType.equals(BicycleBigQueryWrapper.INCREMENTAL_SYNC_TYPE)) {
                this.cursorField = syncMode.has(CURSOR_FIELD) ?
                        syncMode.get(CURSOR_FIELD).asText() : null;
            }
        }

        initializeDataFormatter(config, this.cursorField);

        if (this.cursorField == null) {
            if (this.dataFormatter != null) {
                this.cursorField = dataFormatter.getCursorFieldName();
            } else {
                this.cursorField = cursorField;
            }
        }
    }

    private void initializeDataFormatter(JsonNode config, String cursorField) {

        try {
            JsonNode dataFormatterObject = config.has(BicycleBigQueryWrapper.DATA_FORMATTER_TYPE)
                    ? config.get(BicycleBigQueryWrapper.DATA_FORMATTER_TYPE) : null;
            if (dataFormatterObject != null) {
                String dataFormatterType = dataFormatterObject.get(BicycleBigQueryWrapper.FORMAT_TYPE).asText();
                if (StringUtils.isNotEmpty(dataFormatterType) && !dataFormatterType.equals("None")) {
                    Map<String, Object> dataFormatterConfigMap = new HashMap<>();
                    if (dataFormatterType.equals("GoogleAnalytics4")) {
                        String unMapColumnsName = dataFormatterObject.has(BicycleBigQueryWrapper.UNMAP_COLUMNS_NAME) ?
                                dataFormatterObject.get(BicycleBigQueryWrapper.UNMAP_COLUMNS_NAME).asText() : null;
                        String matchStreamNames = dataFormatterObject.has(BicycleBigQueryWrapper.MATCH_STREAMS_NAME) ?
                                dataFormatterObject.get(BicycleBigQueryWrapper.MATCH_STREAMS_NAME).asText() : null;
                        if (StringUtils.isNotEmpty(unMapColumnsName)) {
                            dataFormatterConfigMap.put(BicycleBigQueryWrapper.UNMAP_COLUMNS_NAME, unMapColumnsName);
                        }
                        if (StringUtils.isNotEmpty(matchStreamNames)) {
                            dataFormatterConfigMap.put(BicycleBigQueryWrapper.MATCH_STREAMS_NAME, matchStreamNames);
                        }
                    }
                    if (cursorField != null) {
                        dataFormatterConfigMap.put(CURSOR_FIELD, cursorField);
                    }

                    DataFormatterConfig dataFormatterConfig = new DataFormatterConfig(dataFormatterConfigMap);
                    dataFormatter = DataFormatterFactory.getDataFormatter(DataFormatterType.valueOf(dataFormatterType),
                            dataFormatterConfig);
                    logger.info("Successfully initialize data formatter {}", dataFormatter);
                }
            }
        } catch (Exception e) {
            logger.error("Unable to initialize data formatter", e);
        }
    }

    public String getProjectId() {
        return projectId;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public String getCredentialsJson() {
        return credentialsJson;
    }

    public int getDefaultLimit() {
        return defaultLimit;
    }

    public String getDefaultCursorValue() {
        return defaultCursorValue;
    }

    public String getCursorField() {
        return cursorField;
    }

    public DataFormatter getDataFormatter() {
        return dataFormatter;
    }
}
