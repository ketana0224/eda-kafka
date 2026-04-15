package com.example.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * Key あり/なしでパーティション振り分けの違いを確認する Producer。
 *
 * Key なし → ラウンドロビンで3パーティションに分散
 * Key あり → 同一 Key は常に同一パーティションに集約（murmur2 ハッシュ）
 *
 * 実行:
 *   # Key なし（ラウンドロビン確認）
 *   mvn exec:java -Dexec.mainClass=com.example.kafka.KeyedProducer -Dexec.args="pubsub-test nokey"
 *
 *   # Key あり（同一パーティション集約確認）
 *   mvn exec:java -Dexec.mainClass=com.example.kafka.KeyedProducer -Dexec.args="pubsub-test keyed"
 *
 *   引数: <topicName> <nokey|keyed>
 */
public class KeyedProducer {

    private static final String[] KEYS = {"user-A", "user-B", "user-A", "user-C", "user-A", "user-B"};

    public static void main(String[] args) throws Exception {
        String topic = args.length > 0 ? args[0] : "pubsub-test";
        String mode  = args.length > 1 ? args[1] : "keyed";
        boolean useKey = !"nokey".equalsIgnoreCase(mode);

        Properties props = TopicAdmin.loadProperties();
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < KEYS.length; i++) {
                String key   = useKey ? KEYS[i] : null;
                String value = "msg-" + (i + 1);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                final int idx = i;
                producer.send(record, (metadata, ex) -> {
                    if (ex != null) {
                        System.err.println("Send failed: " + ex.getMessage());
                    } else {
                        System.out.printf("[%s] key=%-8s value=%-8s -> partition=%d offset=%d%n",
                                mode, String.valueOf(key), value,
                                metadata.partition(), metadata.offset());
                    }
                });
            }
            producer.flush();
        }
        System.out.println("Done. mode=" + mode);
        System.out.println("期待: " + (useKey
                ? "user-A のメッセージがすべて同一パーティションに集約されること"
                : "メッセージが複数パーティションに分散されること"));
    }
}
