package com.github.wojciech.fiszer.kafka.streams.workshop.streams;

import com.github.wojciech.fiszer.kafka.streams.workshop.avro.Event;
import com.github.wojciech.fiszer.kafka.streams.workshop.avro.UserKey;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class EventProducer {

    private static final Logger log = LoggerFactory.getLogger(EventProducer.class);

    private static final List<Long> USER_IDS = Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L);
    private static final List<String> SEARCH_QUERIES = Arrays.asList("computer", "book", "cd", "phone");
    private static final List<Long> OFFER_IDS = Arrays.asList(1L, 2L, 3L);
    static final List<String> OFFER_INTERACTIONS = Arrays.asList("buyNow", "bid");

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class.getName());
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        Producer<UserKey, Event> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10000; i++) {
            // we want to preserve order of changes of each user, so id will be a good key
            UserKey key = UserKey.newBuilder().setId(getRandom(USER_IDS)).build();
            Event value = randomEvent();
            Future<RecordMetadata> recordMetadataFuture = producer.send(new ProducerRecord<>("event", key, value));
            // do not do that on production
            RecordMetadata recordMetadata = recordMetadataFuture.get();
            log.info("Sent record key: {}, value: {} to topic: {}, partition: {}", key, value, recordMetadata.topic(), recordMetadata.partition());
        }
        producer.close();

    }

    private static Event randomEvent() {
        return ThreadLocalRandom.current().nextInt() % 2 == 0 ? randomSearchQueryEvent() : randomOfferInteractionEvent();
    }

    private static Event randomSearchQueryEvent() {
        return Event.newBuilder()
                .setType("searchQuery")
                .setSearchQuery(getRandom(SEARCH_QUERIES))
                .build();
    }

    private static Event randomOfferInteractionEvent() {
        return Event.newBuilder()
                .setType(getRandom(OFFER_INTERACTIONS))
                .setOfferId(getRandom(OFFER_IDS))
                .build();
    }

    private static <T> T getRandom(List<T> list) {
        return list.get(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE) % list.size());
    }
}
