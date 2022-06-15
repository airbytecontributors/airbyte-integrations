package io.airbyte.integrations.source.elasticsearch;

import java.util.Arrays;
import java.util.List;

public class ElasticsearchInclusions {
    private static final String type = "type";
    private static final String properties = "properties";
    public static final List<String> KEEP_LIST = Arrays.asList(type, properties);
}
