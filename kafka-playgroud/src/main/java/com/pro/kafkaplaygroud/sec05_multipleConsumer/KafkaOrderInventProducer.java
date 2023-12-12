package com.pro.kafkaplaygroud.sec05_multipleConsumer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;

public class KafkaOrderInventProducer {
    public static final Logger logger = LoggerFactory.getLogger(KafkaOrderInventProducer.class);

    public static void main(String[] args) {

        //options

        Map<String, Object> config = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        //sender options
        var senderOptions = SenderOptions.<String, String>create(config)
                .maxInFlight(500);
        //flux
        var flux = Flux.range(1, 10000)
                .map(x -> new ProducerRecord<>("order-events", x.toString(), x.toString()))
                .map(x -> SenderRecord.create(x, x.key()));


        //send
        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);
        sender.send(flux)
                .delayElements(Duration.ofMillis(50))
                .doOnNext(x -> logger.info(" correliation id: {}", x.recordMetadata()))
                .doOnComplete(sender::close)
                .subscribe();


    }
}
