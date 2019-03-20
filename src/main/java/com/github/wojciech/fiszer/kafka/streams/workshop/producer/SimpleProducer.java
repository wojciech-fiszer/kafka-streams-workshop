package com.github.wojciech.fiszer.kafka.streams.workshop.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleProducer {

    private static final Logger log = LoggerFactory.getLogger(SimpleProducer.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(properties);
        List<String> names = Arrays.asList("John", "Ann", "Mark", "Stephen", "Julia", "Max");
        List<String> colors = Arrays.asList("red", "blue", "green", "yellow");

        for (int i = 0; i < 100; i++) {
            // we want to preserve order of changes of each user, so 'unique' name will be a good key
            String key = names.get(i % names.size());
            String value = colors.get(i % colors.size());
            Future<RecordMetadata> recordMetadataFuture = producer.send(new ProducerRecord<>("test", key, value));
            // do not do that on production
            RecordMetadata recordMetadata = recordMetadataFuture.get();
            log.info("Sent record key: {}, value: {} to topic: {}, partition: {}", key, value, recordMetadata.topic(), recordMetadata.partition());
        }
        producer.close();
    }
}
