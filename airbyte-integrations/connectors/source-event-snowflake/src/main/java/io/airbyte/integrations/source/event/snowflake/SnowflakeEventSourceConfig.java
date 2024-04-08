package io.airbyte.integrations.source.event.snowflake;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;

/**
 * @author sumitmaheshwari
 * Created on 13/10/2023
 */
public class SnowflakeEventSourceConfig {

    private final String accountName;
    private final String role;
    private final String warehouse;
    private final String database;
    private final String schema;
    private SnowflakeAuthInfo snowflakeAuthInfo;
    private boolean isIncremental = true;
    private String cursorFieldFormat;
    private String cursorField;


    public SnowflakeEventSourceConfig(JsonNode config) {
        this.accountName = config.has(BicycleSnowflakeConstants.ACCOUNT_NAME) ?
                config.get(BicycleSnowflakeConstants.ACCOUNT_NAME).asText() : null;
        this.role = config.has(BicycleSnowflakeConstants.ROLE) ?
                config.get(BicycleSnowflakeConstants.ROLE).asText() : null;
        this.warehouse = config.has(BicycleSnowflakeConstants.WAREHOUSE) ?
                config.get(BicycleSnowflakeConstants.WAREHOUSE).asText() : null;
        this.database = config.has(BicycleSnowflakeConstants.DATABASE) ?
                config.get(BicycleSnowflakeConstants.DATABASE).asText() : null;
        this.schema = config.has(BicycleSnowflakeConstants.SCHEMA) ?
                config.get(BicycleSnowflakeConstants.SCHEMA).asText() : null;

        if (config.has(BicycleSnowflakeConstants.CREDENTIALS)) {
            final JsonNode credentials = config.get(BicycleSnowflakeConstants.CREDENTIALS);
            final String authType =
                    credentials.has(BicycleSnowflakeConstants.AUTH_TYPE) ?
                            credentials.get(BicycleSnowflakeConstants.AUTH_TYPE).asText() : "";
            switch (authType) {
                case BicycleSnowflakeConstants.OAUTH -> snowflakeAuthInfo = buildOAuthSnowflakeAuthInfo(authType,
                        credentials);
                case BicycleSnowflakeConstants.USERNAME_PASSWORD -> snowflakeAuthInfo =
                        buildUsernamePasswordSnowflakeAuthInfo(authType, credentials);
                default -> throw new IllegalArgumentException("Unrecognized auth type: " + authType);
            }
        }

        JsonNode syncMode = config.has(BicycleSnowflakeConstants.SYNC_MODE)
                ? config.get(BicycleSnowflakeConstants.SYNC_MODE) : null;
        if (syncMode != null) {
            String syncType = syncMode.get(BicycleSnowflakeConstants.SYNC_TYPE).asText();
            if (StringUtils.isNotEmpty(syncType) && syncType.equals(BicycleSnowflakeConstants.INCREMENTAL_SYNC_TYPE)) {
                isIncremental = true;
                this.cursorField = syncMode.has(BicycleSnowflakeConstants.CURSOR_FIELD) ?
                        syncMode.get(BicycleSnowflakeConstants.CURSOR_FIELD).asText() : null;
                this.cursorFieldFormat = syncMode.has(BicycleSnowflakeConstants.CURSOR_FIELD_FORMAT) ?
                        syncMode.get(BicycleSnowflakeConstants.CURSOR_FIELD_FORMAT).asText() : null;
            } else if (StringUtils.isNotEmpty(syncType) && syncType.equals(BicycleSnowflakeConstants.FULL_SYNC_TYPE)) {
                isIncremental = false;
            }
        }
    }

    private SnowflakeAuthInfo buildOAuthSnowflakeAuthInfo(String authType, JsonNode config) {

        return null;

    }

    private SnowflakeAuthInfo buildUsernamePasswordSnowflakeAuthInfo(String authType, JsonNode config) {

        String userName = config.get(BicycleSnowflakeConstants.USER_NAME).asText();
        String password = config.get(BicycleSnowflakeConstants.PASSWORD).asText();

        return new SnowflakeAuthInfo(authType, new SnowflakeAuthInfo.UserNamePassword(userName, password));

    }

    public String getAccountName() {
        return accountName;
    }

    public String getRole() {
        return role;
    }

    public String getWarehouse() {
        return warehouse;
    }

    public String getDatabase() {
        return database;
    }

    public String getSchema() {
        return schema;
    }

    public SnowflakeAuthInfo getSnowflakeAuthInfo() {
        return snowflakeAuthInfo;
    }

    public String getCursorField() {
        return cursorField;
    }

    public boolean isIncremental() {
        return isIncremental;
    }

    public String getCursorFieldFormat() {
        return cursorFieldFormat;
    }
}
