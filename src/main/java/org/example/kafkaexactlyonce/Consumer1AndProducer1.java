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

public class Consumer1AndProducer1 {

    public static void main(String[] args) {

        String groupId = "some-java-consumer" + UUID.randomUUID();

        Map<String, Object> prodConfig = new HashMap<>(producerConfig);
        prodConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "ex");
        prodConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST2);

        Map<String, Object> consConfig = new HashMap<>(consumerConfig);
        consConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        try (var consumer = new KafkaConsumer<String, String>(consConfig);
             var producer = new KafkaProducer<String, String>(prodConfig)) {
            consumer.subscribe(Arrays.asList("topic-1"));
            producer.initTransactions();
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n",
                            record.offset(), record.key(), record.value());

                    producer.beginTransaction();
                    producer.send(
                            new ProducerRecord<>("topic-2", record.key(), record.value() + "-processed"));
                    producer.sendOffsetsToTransaction(
                            Map.of(
                                    new TopicPartition(record.topic(), record.partition()),
                                    new OffsetAndMetadata(record.offset())),
                                    new ConsumerGroupMetadata(groupId));
                    producer.commitTransaction();
                }
            }
        }
    }
}
