package io.airbyte.integrations.bicycle.base.integration.destinations.writers.impl;

import io.bicycle.integration.connector.DestinationKafka;
import io.bicycle.server.event.mapping.ProcessedRawEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

/**
 * @author piyush.moolchandani@bicycle.io
 */
public class KafkaWriter extends BaseWriter {

    private final String uniqueIdentifier;
    private final String topic;
    private final KafkaProducer<String, byte[]> kafkaProducer;

    public KafkaWriter(DestinationKafka destinationKafka) {
        this.uniqueIdentifier = destinationKafka.getPartitionKey();
        this.topic = destinationKafka.getTopicName();
        this.kafkaProducer = buildKafkaProducer(destinationKafka);
    }

    /**
     * Send data to a specified destination.
     */
    @Override
    public boolean writeEventData(boolean isLast, String sourceId, List<ProcessedRawEvent> processedRawEvents) {
        if (processedRawEvents == null || processedRawEvents.size() == 0) {
            return false;
        }
        int numberOfEvents = processedRawEvents.size();
        for (int eventNum = 0; eventNum < numberOfEvents; eventNum += 1) {
            if (isLast && eventNum == numberOfEvents - 1) {
                writeToKafka(true, sourceId, processedRawEvents.get(eventNum));
            }
            writeToKafka(false, sourceId, processedRawEvents.get(eventNum));
        }
        return true;
    }

    @Override
    public boolean writeEntityData(boolean isLast, String sourceId, List<String> records) {
        return false;
    }

    @Override
    public String getUniqueIdentifier() {
        return this.uniqueIdentifier;
    }

    private void writeToKafka(boolean isLast, String sourceId, ProcessedRawEvent processedRawEvent) {
        writeToKafka(this.buildEventData(isLast, sourceId, processedRawEvent).toByteArray());
    }

    private void writeToKafka(byte[] value) {
        writeToKafka(getProducerRecord(value));
    }

    private void writeToKafka(ProducerRecord<String, byte[]> record) {
        this.kafkaProducer.send(record);
    }

    private ProducerRecord<String, byte[]> getProducerRecord(byte[] value) {
        return new ProducerRecord<>(this.topic, this.uniqueIdentifier, value);
    }

    private KafkaProducer<String, byte[]> buildKafkaProducer(DestinationKafka destinationKafka) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, destinationKafka.getBootstrapServers());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BytesSerializer.class.getName());
        return new KafkaProducer<>(properties);
    }
}
