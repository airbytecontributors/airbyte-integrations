package io.airbyte.integrations.bicycle.base.integration.destinations.writers.api;

import io.bicycle.server.event.mapping.ProcessedRawEvent;

import java.util.List;

/**
 * @author piyush.moolchandani@bicycle.io
 */
public interface Writer {

    boolean writeEventData(boolean isLast, String sourceId, List<ProcessedRawEvent> processedRawEvents);

    boolean writeEntityData(boolean isLast, String sourceId, List<String> records);
}
