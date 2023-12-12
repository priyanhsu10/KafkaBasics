package com.pro.kafkaplaygroud.sec05_multipleConsumer;

public class KafkaEventConsumerGroup {

    private static class Consumer1 {
        public static void main(String[] args) {
            KafkaOrderEventConsumer.start("1");
        }
    }

    private static class Consumer2 {
        public static void main(String[] args) {
            KafkaOrderEventConsumer.start("2");
        }
    }

    private static class Consumer3 {
        public static void main(String[] args) {
            KafkaOrderEventConsumer.start("3");
        }
    }
}

