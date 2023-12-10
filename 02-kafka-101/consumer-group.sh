kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hello-world --property print.offset=true --property print.timestamp=true --property print.key=true --group ps --from-beginning 

kafka-cumer-groups.sh --bootstrap-server localhost:9092 --list