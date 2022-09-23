package io.airbyte.integrations.bicycle.base.integration;

import java.util.UUID;

public class CommonUtils {
    public static String getRandomBicycleUUID() {
        return "bicycle_"+ UUID.randomUUID().toString();
    }
}