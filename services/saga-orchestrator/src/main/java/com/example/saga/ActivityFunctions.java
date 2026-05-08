package com.example.saga;

import com.example.saga.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Properties;

/**
 * Saga の各ステップを担う Activity Functions。
 *
 *  CreateOrder                    - OrderService に HTTP POST して orderId を返す
 *  PublishInventoryReserveCommand - inventory.reserve.command を Kafka に発行
 *  PublishInventoryRelease        - inventory.release.command を Kafka に発行（補償）
 *  PublishShippingScheduleCommand - shipping.schedule.command を Kafka に発行
 *  PublishOrderConfirmed          - order.confirmed を Kafka に発行
 *  PublishOrderCancelled          - order.cancelled を Kafka に発行（補償）
 */
public class ActivityFunctions {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    // Kafka プロデューサーはスレッドセーフのため static で保持
    private static volatile KafkaProducer<String, String> kafkaProducer;

    // -------------------------------------------------------------------------
    // ① CreateOrder
    // -------------------------------------------------------------------------
    @FunctionName("CreateOrder")
    public String createOrder(
            @DurableActivityTrigger(name = "taskActivityContext")
            SagaInput input,
            ExecutionContext context) throws Exception {

        String orderServiceUrl = System.getenv("ORDER_SERVICE_URL");
        if (orderServiceUrl == null) throw new IllegalStateException("ORDER_SERVICE_URL is not set");

        // items を OrderService の期待する形式にシリアライズ
        String body = MAPPER.writeValueAsString(Map.of(
                "customerId", input.customerId(),
                "shippingAddress", input.shippingAddress(),
                "items", input.items(),
                "totalAmount", input.totalAmount()));

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(orderServiceUrl + "/api/orders"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> resp = HTTP_CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new RuntimeException("OrderService returned HTTP " + resp.statusCode()
                    + ": " + resp.body());
        }

        CreateOrderResponse orderResp = MAPPER.readValue(resp.body(), CreateOrderResponse.class);
        context.getLogger().info("Order created: orderId=" + orderResp.orderId());
        return orderResp.orderId();
    }

    // -------------------------------------------------------------------------
    // ② PublishInventoryReserveCommand
    // -------------------------------------------------------------------------
    @FunctionName("PublishInventoryReserveCommand")
    public void publishInventoryReserveCommand(
            @DurableActivityTrigger(name = "taskActivityContext")
            InventoryReserveCommand cmd,
            ExecutionContext context) throws Exception {
        String json = MAPPER.writeValueAsString(cmd);
        sendKafka("inventory.reserve.command", cmd.orderId(), json);
        context.getLogger().info("inventory.reserve.command published: orderId=" + cmd.orderId());
    }

    // -------------------------------------------------------------------------
    // ③ PublishInventoryRelease（補償）
    // -------------------------------------------------------------------------
    @FunctionName("PublishInventoryRelease")
    public void publishInventoryRelease(
            @DurableActivityTrigger(name = "taskActivityContext")
            InventoryReleaseCommand cmd,
            ExecutionContext context) throws Exception {
        String json = MAPPER.writeValueAsString(cmd);
        sendKafka("inventory.release.command", cmd.orderId(), json);
        context.getLogger().info("inventory.release.command published: orderId=" + cmd.orderId());
    }

    // -------------------------------------------------------------------------
    // ④ PublishShippingScheduleCommand
    // -------------------------------------------------------------------------
    @FunctionName("PublishShippingScheduleCommand")
    public void publishShippingScheduleCommand(
            @DurableActivityTrigger(name = "taskActivityContext")
            ShippingScheduleCommand cmd,
            ExecutionContext context) throws Exception {
        String json = MAPPER.writeValueAsString(cmd);
        sendKafka("shipping.schedule.command", cmd.orderId(), json);
        context.getLogger().info("shipping.schedule.command published: orderId=" + cmd.orderId());
    }

    // -------------------------------------------------------------------------
    // ⑤ PublishOrderConfirmed
    // -------------------------------------------------------------------------
    @FunctionName("PublishOrderConfirmed")
    public void publishOrderConfirmed(
            @DurableActivityTrigger(name = "taskActivityContext")
            OrderConfirmedCommand cmd,
            ExecutionContext context) throws Exception {
        String json = MAPPER.writeValueAsString(cmd);
        sendKafka("order.confirmed", cmd.orderId(), json);
        context.getLogger().info("order.confirmed published: orderId=" + cmd.orderId());
    }

    // -------------------------------------------------------------------------
    // ⑥ PublishOrderCancelled（補償）
    // -------------------------------------------------------------------------
    @FunctionName("PublishOrderCancelled")
    public void publishOrderCancelled(
            @DurableActivityTrigger(name = "taskActivityContext")
            OrderCancelledCommand cmd,
            ExecutionContext context) throws Exception {
        String json = MAPPER.writeValueAsString(cmd);
        sendKafka("order.cancelled", cmd.orderId(), json);
        context.getLogger().info("order.cancelled published: orderId=" + cmd.orderId()
                + " reason=" + cmd.reason());
    }

    // -------------------------------------------------------------------------
    // Kafka 送信ヘルパー
    // -------------------------------------------------------------------------
    private static void sendKafka(String topic, String key, String value) throws Exception {
        getKafkaProducer().send(new ProducerRecord<>(topic, key, value)).get();
    }

    private static KafkaProducer<String, String> getKafkaProducer() {
        if (kafkaProducer == null) {
            synchronized (ActivityFunctions.class) {
                if (kafkaProducer == null) {
                    String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
                    if (bootstrapServers == null) {
                        throw new IllegalStateException("KAFKA_BOOTSTRAP_SERVERS is not set");
                    }
                    Properties props = new Properties();
                    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                            StringSerializer.class.getName());
                    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                            StringSerializer.class.getName());
                    props.put(ProducerConfig.ACKS_CONFIG, "all");
                    props.put(ProducerConfig.RETRIES_CONFIG, "3");
                    kafkaProducer = new KafkaProducer<>(props);
                }
            }
        }
        return kafkaProducer;
    }
}
