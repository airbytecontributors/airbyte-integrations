package io.airbyte.integrations.bicycle.base.integration;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.UUID;

public class CommonUtils {
    private static ObjectMapper objectMapper = new ObjectMapper();
    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }
    public static final String UNKNOWN_EVENT_CONNECTOR = "UNKNOWN";
    public static String getRandomBicycleUUID() {
        return "bicycle_"+ UUID.randomUUID().toString();
    }
}