package io.airbyte.integrations.source.event.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

/**
 * @author sumitmaheshwari
 * Created on 13/10/2023
 */
public class BigQueryEventSourceConfig {

    private final String projectId;
    private final String datasetId;
    private final String credentialsJson;
    private final List<String> streamNames;
    private final String defaultCursorValue;
    private final int defaultLimit;
    private final String cursorField;

    public BigQueryEventSourceConfig(JsonNode config, List<String> streamNames, String cursorField) {
        this.projectId = config.has(BicycleBigQueryWrapper.CONFIG_PROJECT_ID) ?
                config.get(BicycleBigQueryWrapper.CONFIG_PROJECT_ID).asText() : null;
        this.datasetId = config.has(BicycleBigQueryWrapper.CONFIG_DATASET_ID) ?
                config.get(BicycleBigQueryWrapper.CONFIG_DATASET_ID).asText() : null;
        this.credentialsJson = config.has(BicycleBigQueryWrapper.CONFIG_CREDS) ?
                config.get(BicycleBigQueryWrapper.CONFIG_CREDS).asText() : null;
        this.streamNames = streamNames;
        this.defaultCursorValue = config.has("cursor_default_value") ?
                config.get("cursor_default_value").asText() : null;
        this.defaultLimit = config.has("fetch_rows_limit") ?
                config.get("fetch_rows_limit").asInt() : 1000;
        this.cursorField = cursorField;

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

    public List<String> getStreamNames() {
        return streamNames;
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
}
