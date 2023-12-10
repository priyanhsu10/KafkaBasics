package com.pro.kafkaplaygroud.sec01;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class Lec02KafkaConsumer {
    private static Logger log = LoggerFactory.getLogger(Lec02KafkaConsumer.class);
    public static void main(String[] args) {


        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"1"
        );

        ReceiverOptions<Object, Object> objectReceiverOptions = ReceiverOptions.create(consumerConfig)
               // .subscription(List.of("order-events","order-inventory"));
   //we can give regular expresssion paternn
                .subscription(Pattern.compile("order.*"));
        KafkaReceiver<Object, Object> kafkaReceiver = KafkaReceiver.create(objectReceiverOptions);
        kafkaReceiver.receive()
                .doOnNext(r -> log.info("topic : {}, key: {} value :{}",r.topic(), r.key(), r.value()))
                .doOnNext(r->r.receiverOffset().acknowledge())
                .subscribe();
    }
}
