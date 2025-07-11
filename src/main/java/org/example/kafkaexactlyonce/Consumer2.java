package org.example.kafkaexactlyonce;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.example.kafkaexactlyonce.Utils.*;

public class Consumer2 {


    public static void main(String[] args) {

        String groupId = "some-java-consumer" + UUID.randomUUID();

        Map<String, Object> consConfig = new HashMap<>(consumerConfig);
        consConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST2);
        consConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (var consumer = new KafkaConsumer<String, String>(consConfig)) {
            consumer.subscribe(Arrays.asList("topic-2"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n",
                            record.offset(), record.key(), record.value());
                }
            }
        }
    }
}

