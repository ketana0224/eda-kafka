package com.example.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * Key なしでメッセージを送信する基本 Producer。
 * パーティションはラウンドロビンで分散される。
 *
 * 実行:
 *   mvn exec:java -Dexec.mainClass=com.example.kafka.SimpleProducer -Dexec.args="pubsub-test 10"
 *   引数: <topicName> [messageCount(default=5)]
 */
public class SimpleProducer {

    public static void main(String[] args) throws Exception {
        String topic = args.length > 0 ? args[0] : "pubsub-test";
        int count = args.length > 1 ? Integer.parseInt(args[1]) : 5;

        Properties props = TopicAdmin.loadProperties();
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= count; i++) {
                String value = "message-" + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
                producer.send(record, (metadata, ex) -> {
                    if (ex != null) {
                        System.err.println("Send failed: " + ex.getMessage());
                    } else {
                        System.out.printf("Sent: value=%-15s partition=%d offset=%d%n",
                                value, metadata.partition(), metadata.offset());
                    }
                });
            }
            producer.flush();
        }
        System.out.println("Done.");
    }
}
