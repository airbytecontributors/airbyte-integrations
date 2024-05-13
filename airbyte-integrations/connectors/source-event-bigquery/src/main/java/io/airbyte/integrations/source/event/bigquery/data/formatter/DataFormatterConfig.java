package io.airbyte.integrations.source.event.bigquery.data.formatter;

import java.util.HashMap;
import java.util.Map;

/**
 * @author sumitmaheshwari
 * Created on 10/05/2024
 */
public class DataFormatterConfig {

    private Map<String, Object> configurations = new HashMap<>();

    public DataFormatterConfig(Map<String, Object> configurations) {
        this.configurations = configurations;
    }

    public Object getConfigValue(String key) {
        return configurations.get(key);
    }

    @Override
    public String toString() {
        return "DataFormatterConfig{" +
                "configurations=" + configurations +
                '}';
    }
}
