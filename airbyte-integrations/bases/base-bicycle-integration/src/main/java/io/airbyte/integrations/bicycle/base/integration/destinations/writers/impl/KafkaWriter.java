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

    private final DestinationKafka destinationKafka;
    private final KafkaProducer<String, byte[]> kafkaProducer;

    public KafkaWriter(DestinationKafka destinationKafka) {
        this.destinationKafka = destinationKafka;
        this.kafkaProducer = buildKafkaProducer(destinationKafka);
    }

    /**
     * Send data to a specified destination.
     */
    public boolean writeEventData(List<ProcessedRawEvent> processedRawEvents) {
        if (processedRawEvents == null || processedRawEvents.size() == 0) {
            return false;
        }
        for (ProcessedRawEvent processedRawEvent: processedRawEvents) {
            writeToKafka(
                    this.destinationKafka.getTopicName(), this.destinationKafka.getPartitionKey(), processedRawEvent);
        }
        return true;
    }

    private void writeToKafka(String kafkaTopic, String partitionKey, ProcessedRawEvent processedRawEvent) {
        writeToKafka(kafkaTopic, partitionKey, this.buildEventData(processedRawEvent).toByteArray());
    }

    private void writeToKafka(String kafkaTopic, String partitionKey, byte[] value) {
        writeToKafka(getProducerRecord(kafkaTopic, partitionKey, value));
    }

    private void writeToKafka(ProducerRecord<String, byte[]> record) {
        this.kafkaProducer.send(record);
    }

    private ProducerRecord<String, byte[]> getProducerRecord(String kafkaTopic, String partitionKey, byte[] value) {
        return new ProducerRecord<>(kafkaTopic, partitionKey, value);
    }

    private KafkaProducer<String, byte[]> buildKafkaProducer(DestinationKafka destinationKafka) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, destinationKafka.getBootstrapServers());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BytesSerializer.class.getName());
        return new KafkaProducer<>(properties);
    }
}
