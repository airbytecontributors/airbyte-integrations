package io.airbyte.integrations.bicycle.base.integration.destinations.writers.impl;

import io.airbyte.integrations.bicycle.base.integration.destinations.writers.api.Writer;
import io.bicycle.integration.connector.DataSyncDestination;

/**
 * @author piyush.moolchandani@bicycle.io
 */
public class WriterFactory {

    public static Writer getWriter(DataSyncDestination dataSyncDestination) {
        if (dataSyncDestination == null) {
            throw new RuntimeException("Destination cannot be null");
        }
        if (dataSyncDestination.getType().equals(DataSyncDestination.Type.KAFKA)) {
            return new KafkaWriter(dataSyncDestination.getDestinationKafka());
        }
        throw new RuntimeException("Unsupported Writer requested");
    }
}
