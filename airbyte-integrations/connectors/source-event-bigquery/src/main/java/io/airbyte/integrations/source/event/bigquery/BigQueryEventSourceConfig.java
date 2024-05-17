package io.airbyte.integrations.source.event.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import static io.airbyte.integrations.source.event.bigquery.BicycleBigQueryWrapper.CUSTOM_DIMENSION_MAPPING_COLUMN_NAME;
import static io.airbyte.integrations.source.event.bigquery.BicycleBigQueryWrapper.CUSTOM_DIMENSION_MAPPING_COLUMN_NAME_DEFAULT;

/**
 * @author sumitmaheshwari
 * Created on 13/10/2023
 */
public class BigQueryEventSourceConfig {

    private static final Logger logger = LoggerFactory.getLogger(BigQueryEventSourceConfig.class.getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final String projectId;
    private final String datasetId;
    private final String credentialsJson;
    private final String defaultCursorValue;
    private final int defaultLimit;
    private String cursorField;
    private DataFormatter dataFormatter;
    private boolean isBackFillEnabled;

    public BigQueryEventSourceConfig(JsonNode config, String cursorField, boolean isBackFillEnabled) {
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
                        syncMode.get(CURSOR_FIELD).asText() : cursorField;
            }
        }

        initializeDataFormatter(config, this.cursorField);

      /*  if (this.cursorField == null) {
            if (this.dataFormatter != null) {
                this.cursorField = dataFormatter.getCursorFieldName();
            } else {
                this.cursorField = cursorField;
            }
        }*/
        this.isBackFillEnabled = isBackFillEnabled;

    }
    public BigQueryEventSourceConfig(JsonNode config, String cursorField) {
       this(config, cursorField, false);
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
                        String customDimensionMapping =
                                dataFormatterObject.has(BicycleBigQueryWrapper.CUSTOM_DIMENSION_MAPPING) ?
                                        dataFormatterObject.get(BicycleBigQueryWrapper.CUSTOM_DIMENSION_MAPPING)
                                                .asText() : null;
                        String customDimensionMappingColumnName =
                                dataFormatterObject.has(CUSTOM_DIMENSION_MAPPING_COLUMN_NAME) ?
                                        dataFormatterObject.get(BicycleBigQueryWrapper.CUSTOM_DIMENSION_MAPPING_COLUMN_NAME)
                                                .asText() : CUSTOM_DIMENSION_MAPPING_COLUMN_NAME_DEFAULT;
                        if (StringUtils.isNotEmpty(unMapColumnsName)) {
                            dataFormatterConfigMap.put(BicycleBigQueryWrapper.UNMAP_COLUMNS_NAME, unMapColumnsName);
                        }
                        if (StringUtils.isNotEmpty(matchStreamNames)) {
                            dataFormatterConfigMap.put(BicycleBigQueryWrapper.MATCH_STREAMS_NAME, matchStreamNames);
                        }
                        dataFormatterConfigMap.put(CUSTOM_DIMENSION_MAPPING_COLUMN_NAME,
                                customDimensionMappingColumnName);
                        if (StringUtils.isNotEmpty(customDimensionMapping)) {
                            try {
                                Map<String, String> customDimensionMappingMap =
                                        objectMapper.readValue(customDimensionMapping, Map.class);
                                dataFormatterConfigMap.put(BicycleBigQueryWrapper.CUSTOM_DIMENSION_MAPPING,
                                        customDimensionMappingMap);
                            } catch (Exception e) {
                                logger.error("Unable to deserialize custom dimension mapping string");
                            }
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

    public boolean isBackFillEnabled() {
        return isBackFillEnabled;
    }

    @Override
    public String toString() {
        return "BigQueryEventSourceConfig{" +
                "projectId='" + projectId + '\'' +
                ", datasetId='" + datasetId + '\'' +
                ", credentialsJson='" + credentialsJson + '\'' +
                ", defaultCursorValue='" + defaultCursorValue + '\'' +
                ", defaultLimit=" + defaultLimit +
                ", cursorField='" + cursorField + '\'' +
                ", dataFormatter=" + dataFormatter +
                ", isBackFillEnabled=" + isBackFillEnabled +
                '}';
    }
}
