package io.airbyte.integrations.source.kinesis;

import io.airbyte.integrations.bicycle.base.integration.BicycleConfig;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

import java.util.List;
import java.util.Map;

public class ShardRecordProcessorFactoryImpl implements ShardRecordProcessorFactory {
    private BicycleConfig bicycleConfig;
    private final EventSourceInfo eventSourceinfo;
    private KinesisSource kinesisSource;
    private KinesisClientConfig kinesisClientConfig;
    private Boolean isPreview;
    private List<Object> objList;
    private Map<String, Long> totalRecordsRead;

    ShardRecordProcessorFactoryImpl(KinesisSource kinesisSource, BicycleConfig bicycleConfig, EventSourceInfo eventSourceInfo, KinesisClientConfig kinesisClientConfig,Boolean isPreview, List<Object> objList, Map<String, Long> totalRecordsRead) {
        this.kinesisSource = kinesisSource;
        this.bicycleConfig = bicycleConfig;
        this.eventSourceinfo = eventSourceInfo;
        this.kinesisClientConfig = kinesisClientConfig;
        this.isPreview = isPreview;
        this.objList = objList;
        this.totalRecordsRead = totalRecordsRead;
    }

    public ShardRecordProcessor shardRecordProcessor() {
//        if (isPreview==true) {
//            return new previewShardRecordProcessor(kinesisSource, bicycleConfig, eventSourceinfo, objList);
//        }
        return new ShardRecordProcessorImpl(kinesisSource, bicycleConfig, eventSourceinfo, kinesisClientConfig, totalRecordsRead);
    }
}
