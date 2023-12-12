package com.pro.kafkaplaygroud.sec05_multipleConsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

public class KafkaOrderEventConsumer {
    public static final Logger logger = LoggerFactory.getLogger(KafkaOrderEventConsumer.class);

    public static void start(String id) {

        var config = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-events",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, id

        );
// Reciever options

        var options = ReceiverOptions.<String, String>create(config).subscription(List.of("order-events"));

        KafkaReceiver.create(options)
                .receive()
                .doOnNext(x -> logger.info(" key: {},  value : {}", x.key(), x.value()))
                .doOnNext(x -> x.receiverOffset().acknowledge())
                .subscribe();

    }
}
