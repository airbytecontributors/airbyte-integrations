package io.airbyte.integrations.bicycle.base.integration.destinations.writers.impl;

import io.airbyte.integrations.bicycle.base.integration.destinations.writers.api.Writer;
import io.bicycle.integration.connector.Data;
import io.bicycle.integration.connector.EntitySourceData;
import io.bicycle.integration.connector.EventSourceData;
import io.bicycle.server.event.mapping.ProcessedRawEvent;

/**
 * @author piyush.moolchandani@bicycle.io
 */
abstract class BaseWriter implements Writer {

    protected Data buildEventData(ProcessedRawEvent processedRawEvent) {
        return Data.newBuilder()
                .setDataSourceType(Data.DataSourceType.EVENT)
                .setEventSourceData(
                        EventSourceData.newBuilder()
                                .setRawEvent(processedRawEvent.getRawEvent())
                                .setBicycleEvent(processedRawEvent.getBicycleEvent())
                                .build()
                ).build();
    }

    protected Data buildEntityData(String record) {
        return Data.newBuilder()
                .setDataSourceType(Data.DataSourceType.ENTITY)
                .setEntitySourceData(
                        EntitySourceData.newBuilder()
                                .setRecord(record)
                                .build()
                ).build();
    }
}
