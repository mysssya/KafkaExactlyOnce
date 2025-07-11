package org.example.kafkaexactlyonce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.UUID;

public class Utils {

    public static final String HOST1 = "localhost:9092";
    public static final String HOST2 = "localhost:9093";

    public static final Map<String, Object> producerConfig = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST1,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.ACKS_CONFIG, "all"
    );


    public static final Map<String, Object> consumerConfig = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST1,
            ConsumerConfig.GROUP_ID_CONFIG, "some-java-consumer" + UUID.randomUUID(),
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

}
