package com.github.wojciech.fiszer.kafka.streams.workshop.streams;

import com.github.wojciech.fiszer.kafka.streams.workshop.avro.Event;
import com.github.wojciech.fiszer.kafka.streams.workshop.avro.OfferKey;
import com.github.wojciech.fiszer.kafka.streams.workshop.avro.UserKey;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.github.wojciech.fiszer.kafka.streams.workshop.streams.EventProducer.OFFER_INTERACTIONS;

public class OfferInteractionStream {

    private static final Logger log = LoggerFactory.getLogger(OfferInteractionStream.class);

    @SuppressWarnings("Duplicates")
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "offer-interaction-stream");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100); // uncomment if we want to flush state more often (shorter delay)

        StreamsBuilder builder = new StreamsBuilder();

        builder.<UserKey, Event>stream("event")
                .filter((key, event) -> OFFER_INTERACTIONS.contains(event.getType()))
                .groupBy((key, event) -> OfferKey.newBuilder().setId(event.getOfferId()).build())
                .count()
                .toStream()
                .peek((offerId, numberOfInteractions) -> log.info("Number of interactions for offer {} is {}", offerId, numberOfInteractions));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
