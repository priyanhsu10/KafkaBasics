kafka-topics.sh --bootstrap-server localhost:9092 --topic hello-world --create

#  list topics
kafka-topics.sh --bootstrap-server localhost:9092 --list

# describe topic

kafka-topics.sh --boostrap-server localhost:9092 --topic hello-world --describe

# create topic with 2 partition 

kafka-topics.sh --bootstrap-server localhost:9092 --topic hello-world --partitions 2 --create