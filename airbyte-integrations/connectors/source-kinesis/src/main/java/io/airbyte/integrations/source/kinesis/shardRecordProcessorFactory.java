package io.airbyte.integrations.source.kinesis;

import com.inception.server.auth.model.AuthInfo;
import io.airbyte.integrations.bicycle.base.integration.BicycleConfig;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

public class shardRecordProcessorFactory implements ShardRecordProcessorFactory {
    private BicycleConfig bicycleConfig;
    private final EventSourceInfo eventSourceinfo;
    private KinesisSource kinesisSource;

    shardRecordProcessorFactory(KinesisSource kinesisSource, BicycleConfig bicycleConfig, EventSourceInfo eventSourceInfo) {
        this.kinesisSource = kinesisSource;
        this.bicycleConfig = bicycleConfig;
        this.eventSourceinfo = eventSourceInfo;
    }

    public ShardRecordProcessor shardRecordProcessor() {
        return new shardRecordProcessor(kinesisSource, bicycleConfig, eventSourceinfo);
    }
}
