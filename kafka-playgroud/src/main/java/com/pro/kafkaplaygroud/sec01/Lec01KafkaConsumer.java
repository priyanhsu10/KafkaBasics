package com.pro.kafkaplaygroud.sec01;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

public class Lec01KafkaConsumer {
    private static Logger log = LoggerFactory.getLogger(Lec01KafkaConsumer.class);
    public static void main(String[] args) {


        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group"
        );

        ReceiverOptions<Object, Object> objectReceiverOptions = ReceiverOptions.create(consumerConfig)
                .subscription(List.of("demo-events"));

        KafkaReceiver<Object, Object> kafkaReceiver = KafkaReceiver.create(objectReceiverOptions);
        kafkaReceiver.receive()
                .doOnNext(r -> log.info("key: {} value {}", r.key(), r.value()))
                .subscribe();
    }
}
