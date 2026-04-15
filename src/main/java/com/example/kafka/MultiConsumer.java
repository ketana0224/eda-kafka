package com.example.kafka;

import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 複数 Consumer の挙動を検証する。
 *
 * モード:
 *   same-group  : 同一 Group に Consumer を2つ起動 → メッセージが分散（各メッセージはどちらか1つが受信）
 *   diff-group  : 別 Group の Consumer を2つ起動 → 各 Group が全メッセージを受信
 *
 * 実行（10件送信後にこちらを実行すること）:
 *   mvn exec:java -Dexec.mainClass=com.example.kafka.MultiConsumer -Dexec.args="pubsub-test same-group 10"
 *   mvn exec:java -Dexec.mainClass=com.example.kafka.MultiConsumer -Dexec.args="pubsub-test diff-group 10"
 *
 *   引数: <topicName> <same-group|diff-group> [maxMessages(default=10)]
 */
public class MultiConsumer {

    public static void main(String[] args) throws Exception {
        String topic   = args.length > 0 ? args[0] : "pubsub-test";
        String mode    = args.length > 1 ? args[1] : "same-group";
        int maxTotal   = args.length > 2 ? Integer.parseInt(args[2]) : 10;

        boolean sameGroup = "same-group".equalsIgnoreCase(mode);
        String group1 = "multi-group-1";
        String group2 = sameGroup ? "multi-group-1" : "multi-group-2";

        System.out.printf("Mode: %s%n", mode);
        System.out.printf("Consumer-1: group=%s%n", group1);
        System.out.printf("Consumer-2: group=%s%n", group2);
        System.out.println(sameGroup
                ? "期待: メッセージが Consumer-1 と Consumer-2 に分散されること"
                : "期待: Consumer-1 と Consumer-2 の両方が全メッセージを受信すること");
        System.out.println("---");

        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger total1 = new AtomicInteger(0);
        AtomicInteger total2 = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        executor.submit(() -> runConsumer("Consumer-1", topic, group1, maxTotal, total1, latch));
        executor.submit(() -> runConsumer("Consumer-2", topic, group2, maxTotal, total2, latch));

        latch.await();
        executor.shutdownNow();

        System.out.println("---");
        System.out.printf("Consumer-1 受信数: %d%n", total1.get());
        System.out.printf("Consumer-2 受信数: %d%n", total2.get());
        System.out.printf("合計: %d%n", total1.get() + total2.get());
    }

    private static void runConsumer(String name, String topic, String groupId,
                                    int maxMessages, AtomicInteger counter, CountDownLatch latch) {
        Properties props;
        try {
            props = TopicAdmin.loadProperties();
        } catch (Exception e) {
            System.err.println(name + ": failed to load properties: " + e.getMessage());
            latch.countDown();
            return;
        }
        props.setProperty("group.id", groupId);

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            int received = 0;
            int emptyPolls = 0;
            while (received < maxMessages && emptyPolls < 5) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
                if (records.isEmpty()) {
                    emptyPolls++;
                    continue;
                }
                emptyPolls = 0;
                for (ConsumerRecord<String, String> r : records) {
                    System.out.printf("[%s] key=%-8s value=%-15s partition=%d offset=%d%n",
                            name, String.valueOf(r.key()), r.value(), r.partition(), r.offset());
                    received++;
                    counter.incrementAndGet();
                    if (received >= maxMessages) break;
                }
            }
        } finally {
            latch.countDown();
        }
    }
}
