package org.example.kafkaexactlyonce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

import static org.example.kafkaexactlyonce.Utils.producerConfig;

public class Producer0 {

    public static void main(String[] args) {

        Map<String, Object> prodConfig1 = new HashMap<>(producerConfig);
        prodConfig1.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,  true);
        try(var producer = new KafkaProducer<String, String>(prodConfig1)){

            for(int i = 20; i < 30; i++){
                producer.send(new ProducerRecord<>("topic-1", "some-" + i));
            }
        }
    }
}
