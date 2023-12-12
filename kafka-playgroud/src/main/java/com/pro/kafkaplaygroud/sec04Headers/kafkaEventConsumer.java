package com.pro.kafkaplaygroud.sec04Headers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

public class kafkaEventConsumer {
    public  static  final Logger logger= LoggerFactory.getLogger(KafkaEventProducer.class);
    public  static void main(String[] args){

        Map<String, Object> config = Map.<String, Object>of(

                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-events",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );

        //Reciever options
        ReceiverOptions<String, String> objectReceiverOptions = ReceiverOptions.<String,String>create(config)
                .subscription(List.of("order-events"));

        //KafkaReciever
        KafkaReceiver.create(objectReceiverOptions)
                .receive()
                .doOnNext(x->logger.info("key: {} ,value :{}",x.key(),x.value()))
                .doOnNext(y->y.headers().forEach(x->logger.info("header : key :{} , value :{}",x.key(),new String(x.value()))))
                .doOnNext(x->x.receiverOffset().acknowledge())
                .subscribe();
    }
}
