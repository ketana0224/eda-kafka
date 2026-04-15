package com.example.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * AdminClient を使って Topic の作成・一覧・削除を行う。
 *
 * 実行:
 *   mvn exec:java -Dexec.mainClass=com.example.kafka.TopicAdmin -Dexec.args="create pubsub-test"
 *   mvn exec:java -Dexec.mainClass=com.example.kafka.TopicAdmin -Dexec.args="list"
 *   mvn exec:java -Dexec.mainClass=com.example.kafka.TopicAdmin -Dexec.args="delete pubsub-test"
 */
public class TopicAdmin {

    private static final int PARTITIONS = 3;
    private static final short REPLICATION_FACTOR = 1;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("usage: TopicAdmin <create|list|delete> [topicName]");
            System.exit(1);
        }

        Properties props = loadProperties();
        try (AdminClient admin = AdminClient.create(props)) {
            String command = args[0];
            switch (command) {
                case "create" -> {
                    String topic = requireTopic(args);
                    createTopic(admin, topic);
                }
                case "list" -> listTopics(admin);
                case "delete" -> {
                    String topic = requireTopic(args);
                    deleteTopic(admin, topic);
                }
                default -> {
                    System.err.println("unknown command: " + command);
                    System.exit(1);
                }
            }
        }
    }

    private static void createTopic(AdminClient admin, String topicName)
            throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(topicName, PARTITIONS, REPLICATION_FACTOR);
        CreateTopicsResult result = admin.createTopics(List.of(newTopic));
        KafkaFuture<Void> future = result.values().get(topicName);
        future.get();
        System.out.printf("Created topic: %s (partitions=%d)%n", topicName, PARTITIONS);
    }

    private static void listTopics(AdminClient admin)
            throws ExecutionException, InterruptedException {
        Set<String> topics = admin.listTopics().names().get();
        if (topics.isEmpty()) {
            System.out.println("No topics found.");
        } else {
            topics.stream().sorted().forEach(System.out::println);
        }
    }

    private static void deleteTopic(AdminClient admin, String topicName)
            throws ExecutionException, InterruptedException {
        admin.deleteTopics(List.of(topicName)).all().get();
        System.out.println("Deleted topic: " + topicName);
    }

    private static String requireTopic(String[] args) {
        if (args.length < 2) {
            System.err.println("topic name is required");
            System.exit(1);
        }
        return args[1];
    }

    static Properties loadProperties() throws Exception {
        Properties props = new Properties();
        try (InputStream in = TopicAdmin.class.getClassLoader()
                .getResourceAsStream("kafka.properties")) {
            props.load(in);
        }
        // 環境変数で上書き可能
        String envBootstrap = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (envBootstrap != null) {
            props.setProperty("bootstrap.servers", envBootstrap);
        }
        return props;
    }
}
