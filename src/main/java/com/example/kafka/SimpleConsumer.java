package com.example.kafka;

import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * 単一 Consumer Group でメッセージを受信する基本 Consumer。
 *
 * 実行:
 *   mvn exec:java -Dexec.mainClass=com.example.kafka.SimpleConsumer -Dexec.args="pubsub-test group-a 10"
 *   引数: <topicName> [groupId(default=group-a)] [maxMessages(default=5)]
 *   Ctrl+C で終了
 */
public class SimpleConsumer {

    public static void main(String[] args) throws Exception {
        String topic   = args.length > 0 ? args[0] : "pubsub-test";
        String groupId = args.length > 1 ? args[1] : "group-a";
        int maxMessages = args.length > 2 ? Integer.parseInt(args[2]) : 5;

        Properties props = TopicAdmin.loadProperties();
        props.setProperty("group.id", groupId);

        System.out.printf("Consuming from topic=%s group=%s (max=%d)%n", topic, groupId, maxMessages);

        int received = 0;
        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            while (received < maxMessages) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                if (records.isEmpty()) {
                    System.out.println("No messages for 5s, waiting...");
                    continue;
                }
                for (ConsumerRecord<String, String> r : records) {
                    System.out.printf("Received: key=%-10s value=%-15s partition=%d offset=%d%n",
                            r.key(), r.value(), r.partition(), r.offset());
                    if (++received >= maxMessages) break;
                }
            }
        }
        System.out.println("Done. Total received: " + received);
    }
}
