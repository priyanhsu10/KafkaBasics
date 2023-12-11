package com.pro.kafkaplaygroud.sec03;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

public class kafkaListener {
public static final Logger logger= LoggerFactory.getLogger(KafkaListener.class);
    public static void main(String[] args) {

        Map<String, Object> consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-events",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1");


        //ConsumerOptions
        var options= ReceiverOptions.<String,Object>create(consumerConfig)
                .subscription(List.of("order-events"));
        KafkaReceiver<String,Object> kafkaReceiver = KafkaReceiver.create(options);

        kafkaReceiver.receive()
                .doOnNext(x->logger.info("message: {}",x.value()))
                .doOnNext(x->x.receiverOffset().acknowledge())
                .subscribe();
        //Kafkaconsumer
    }
}
