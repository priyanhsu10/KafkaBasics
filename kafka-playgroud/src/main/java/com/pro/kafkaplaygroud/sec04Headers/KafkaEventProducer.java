package com.pro.kafkaplaygroud.sec04Headers;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Map;

public class KafkaEventProducer {
public static final Logger logger= LoggerFactory.getLogger(KafkaEventProducer.class);
    public static void main(String[] args) {

        //map options
        var config = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        //sender record

        var senderOptions = SenderOptions.<String, String>create(config);

        //flux stream
        var flux = Flux.range(1, 10)
                .map(KafkaEventProducer::createRecord);
        //kafka sender

        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);

        sender.send(flux)
                .doOnNext(x->logger.info(" corilation id: {}", x.recordMetadata()))
                .doOnComplete(sender::close)
                .subscribe();



    }

    public static SenderRecord<String, String, String> createRecord(int value) {

        var headers = new RecordHeaders();
        headers.add("client-id", "some-client".getBytes());
        headers.add("tracing-id", ("abc-" + value).getBytes());

        var producerRecord = new ProducerRecord<>("order-events", null, String.valueOf(value), "order-event-" + value, headers);
        return SenderRecord.create(producerRecord, producerRecord.key());
    }
}