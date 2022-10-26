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

    protected Data buildEventData(boolean isLast, String sourceId, ProcessedRawEvent processedRawEvent) {
        return Data.newBuilder()
                .setRequestId(this.getUniqueIdentifier())
                .setSourceId(sourceId)
                .setDataSourceType(Data.DataSourceType.EVENT)
                .setIsLast(isLast)
                .setEventSourceData(
                        EventSourceData.newBuilder()
                                .setRawEvent(processedRawEvent.getRawEvent())
                                .setBicycleEvent(processedRawEvent.getBicycleEvent())
                                .build()
                ).build();
    }

    protected Data buildEntityData(boolean isLast, String sourceId, String record) {
        return Data.newBuilder()
                .setRequestId(this.getUniqueIdentifier())
                .setSourceId(sourceId)
                .setDataSourceType(Data.DataSourceType.ENTITY)
                .setIsLast(isLast)
                .setEntitySourceData(
                        EntitySourceData.newBuilder()
                                .setRecord(record)
                                .build()
                ).build();
    }

    protected abstract String getUniqueIdentifier();
}
