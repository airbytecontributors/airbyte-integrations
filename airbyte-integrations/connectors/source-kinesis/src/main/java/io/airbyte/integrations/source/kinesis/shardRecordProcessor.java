package io.airbyte.integrations.source.kinesis;

import com.inception.server.auth.model.AuthInfo;
import io.airbyte.integrations.bicycle.base.integration.BicycleAuthInfo;
import io.airbyte.integrations.bicycle.base.integration.BicycleConfig;
import io.bicycle.server.event.mapping.models.converter.BicycleEventsResult;
import io.bicycle.server.event.mapping.models.processor.EventProcessorResult;
import io.bicycle.server.event.mapping.models.processor.EventSourceInfo;
import io.bicycle.server.event.mapping.rawevent.api.RawEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class shardRecordProcessor implements ShardRecordProcessor {

    private String shardId;
    public static final Logger logger = LoggerFactory.getLogger(KinesisSource.class);
    private KinesisSource kinesisSource;
    private BicycleConfig bicycleConfig;
    private EventSourceInfo eventSourceinfo;

    public shardRecordProcessor(KinesisSource kinesisSource, BicycleConfig bicycleConfig, EventSourceInfo eventSourceinfo) {
        this.kinesisSource = kinesisSource;
        this.bicycleConfig = bicycleConfig;
        this.eventSourceinfo = eventSourceinfo;
    }

    public void initialize(InitializationInput initializationInput) {
        shardId = initializationInput.shardId();
        logger.debug("Initializing @ Sequence: " + initializationInput.extendedSequenceNumber());
    }

    public void processRecords(ProcessRecordsInput processRecordsInput) {
        try {
            final List<String> recordsList = new ArrayList<>();
            logger.debug("Processing " + processRecordsInput.records().size() + " record(s)");
            for (KinesisClientRecord record : processRecordsInput.records()) {
                ByteBuffer dup = record.data().duplicate();
                byte[] data = new byte[dup.remaining()];
                dup.get(data);
                String recordString = new String(data, StandardCharsets.UTF_8);
                recordsList.add(recordString);
                logger.info("Processing record pk: " + record.partitionKey() + " shardID: " + shardId + " -- data: " + recordString);
            }
            if (recordsList.size() == 0) {
                return;
            }
            logger.info("No of records read from client are {} ", recordsList.size());
            EventProcessorResult eventProcessorResult = null;
            AuthInfo authInfo = bicycleConfig.getAuthInfo();
            try {
                List<RawEvent> rawEvents = this.kinesisSource.convertRecordsToRawEvents(recordsList);
                eventProcessorResult = this.kinesisSource.convertRawEventsToBicycleEvents(authInfo, eventSourceinfo, rawEvents);
            } catch (Exception exception) {
                logger.error("Unable to convert raw records to bicycle events", exception);
            }

            try {
                this.kinesisSource.publishEvents(authInfo, eventSourceinfo, eventProcessorResult);
                processRecordsInput.checkpointer().checkpoint();
            } catch (Exception exception) {
                logger.error("Unable to publish bicycle events", exception);
            }
        } catch (Throwable t) {
            logger.error("Caught throwable while processing records. Aborting.");
        }
    }

    public void leaseLost(LeaseLostInput leaseLostInput) {
        try {
            logger.debug("Lost lease, so terminating.");
        } catch (Exception e) {
            logger.error("Exception in leaseLost function: ", e);
        }
    }

    public void shardEnded(ShardEndedInput shardEndedInput) {
        try {
            logger.debug("Reached shard end checkpointing.");
            shardEndedInput.checkpointer().checkpoint();
        } catch (software.amazon.kinesis.exceptions.ShutdownException e) {
            logger.error("Exception in shardEnded function: ", e);
        } catch (software.amazon.kinesis.exceptions.InvalidStateException e) {
            logger.error("Exception in shardEnded function: ", e);
        }
    }

    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        try {
            logger.debug("Scheduler is shutting down, checkpointing.");
            shutdownRequestedInput.checkpointer().checkpoint();
        } catch (ShutdownException e) {
            logger.error("Exception in shutdownRequested function: ", e);
        } catch (InvalidStateException e) {
            logger.error("Exception in shutdownRequested function: ", e);
        }
    }
}
