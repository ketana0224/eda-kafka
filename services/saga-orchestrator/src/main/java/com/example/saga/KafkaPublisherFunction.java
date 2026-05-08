package com.example.saga;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Optional;
import java.util.Properties;

/**
 * Logic App から呼び出される Kafka 発行エンドポイント。
 *
 * POST /api/kafka/publish
 * Body: { "topic": "...", "key": "...", "value": "..." }
 *
 * Logic App の OrderSagaOrchestrator が Kafka コマンドを発行する際に使用する。
 * Kafka Managed Connector が Logic Apps Consumption で利用できないため、
 * このエンドポイントが代替ブリッジとして機能する。
 */
public class KafkaPublisherFunction {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static volatile KafkaProducer<String, String> kafkaProducer;

    @FunctionName("KafkaPublish")
    public HttpResponseMessage kafkaPublish(
            @HttpTrigger(
                    name = "req",
                    methods = {HttpMethod.POST},
                    route = "kafka/publish",
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            ExecutionContext context) {

        String body = request.getBody().orElse("{}");

        String topic;
        String key;
        String value;
        try {
            JsonNode node = MAPPER.readTree(body);
            topic = getRequired(node, "topic");
            key   = node.has("key") ? node.get("key").asText("") : "";
            value = getRequired(node, "value");
        } catch (Exception e) {
            context.getLogger().warning("Invalid request body: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body("Invalid body: " + e.getMessage())
                    .build();
        }

        try {
            getProducer().send(new ProducerRecord<>(topic, key.isEmpty() ? null : key, value)).get();
            context.getLogger().info("Kafka published: topic=" + topic + " key=" + key);
            return request.createResponseBuilder(HttpStatus.OK)
                    .body("{\"status\":\"published\"}")
                    .header("Content-Type", "application/json")
                    .build();
        } catch (Exception e) {
            context.getLogger().severe("Kafka publish failed: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Kafka publish failed: " + e.getMessage())
                    .build();
        }
    }

    private static String getRequired(JsonNode node, String field) {
        JsonNode n = node.get(field);
        if (n == null || n.isNull()) {
            throw new IllegalArgumentException("'" + field + "' is required");
        }
        return n.asText();
    }

    private static KafkaProducer<String, String> getProducer() {
        if (kafkaProducer == null) {
            synchronized (KafkaPublisherFunction.class) {
                if (kafkaProducer == null) {
                    String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
                    if (bootstrapServers == null) {
                        throw new IllegalStateException("KAFKA_BOOTSTRAP_SERVERS is not set");
                    }
                    Properties props = new Properties();
                    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                    props.put(ProducerConfig.ACKS_CONFIG, "all");
                    props.put(ProducerConfig.RETRIES_CONFIG, "3");
                    kafkaProducer = new KafkaProducer<>(props);
                }
            }
        }
        return kafkaProducer;
    }
}
