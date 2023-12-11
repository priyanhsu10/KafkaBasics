
package com.pro.kafkaplaygroud.sec03;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Map;

public class KafkaProducer {
    public static final Logger logger= LoggerFactory.getLogger(KafkaProducer.class);
    public static void main(String[] args) {
       var config = Map.<String,Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
                );

       var options = SenderOptions.<String,String>create(config)
               .maxInFlight(20_000);

       var flux = Flux.range(1,1_000_000)
               .map(x->new ProducerRecord<>("order-events",x.toString(),"order-"+x))
               .map(p-> SenderRecord.create(p,p.key()));

       var start= System.currentTimeMillis();
       var sender= KafkaSender.create(options);
       sender.send(flux)
               .doOnNext(r->logger.info("correlation id: {}",r.recordMetadata()))
               .doOnComplete(()->{
                   logger.info("totol time taken :{} ms",System.currentTimeMillis()-start);
                   sender.close();
               })
               .subscribe();


    }
}
