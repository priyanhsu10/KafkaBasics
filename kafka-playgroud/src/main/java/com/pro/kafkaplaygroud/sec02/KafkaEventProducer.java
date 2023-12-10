package com.pro.kafkaplaygroud.sec02;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;

public class KafkaEventProducer {

    public static final Logger log = LoggerFactory.getLogger(KafkaEventProducer.class);

    public static void main(String[] args) {
        //config map
        Map<String, Object> senderConfig = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class

        );
        //options
 var senderOptions = SenderOptions.<String,String>create(senderConfig);
        //producer
var flux = Flux.range(1, 100)
                .delayElements(Duration.ofMillis(500))
                .map(x -> new ProducerRecord<>("order-events", x.toString(),"order-" + x))
                .map(p -> SenderRecord.create(p, p.key()));


        KafkaSender.create(senderOptions)
                .send(flux)
                .doOnNext(r->log.info("correlation Metadata id: {}",r.recordMetadata()))
                .subscribe();
    }
}
