package io.airbyte.integrations.source.elasticsearch;

public class ElasticsearchConstants {
    private ElasticsearchConstants() {}
    public static final String REGEX_FOR_USER_INDICES_ONLY = "(^\\.)|(metrics-endpoint.metadata_current_default)";
    public static final String ALL_INDICES_QUERY = "*";

    public static final String TIME_RANGE = "timeRange";
    public static final String FROM = "from";
    public static final String TO = "to";
    public static final String TIME_FIELD = "timeField";
    public static final String NOW = "now";
    public static final String CONNECTOR_TYPE = "connectorType";
    public static final String ENTITY = "entity";
    public static final String EVENT = "event";
    public static final String SYNCMODE = "syncMode";
    public static final String CURSORFIELD = "cursorField";


    public static final String ES_DEFAULT_TIME_FIELD = "@timestamp";
}
