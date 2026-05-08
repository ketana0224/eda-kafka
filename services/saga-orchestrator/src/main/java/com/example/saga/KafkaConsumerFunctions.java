package com.example.saga;

import com.example.saga.model.InventoryReservationFailedMsg;
import com.example.saga.model.InventoryReservedMsg;
import com.example.saga.model.InventoryResultEvent;
import com.example.saga.model.ShippingResultEvent;
import com.example.saga.model.ShippingScheduledMsg;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.Cardinality;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.KafkaTrigger;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.DurableTaskClient;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * Kafka レスポンスイベントを受信し、以下両方を実行する Functions。
 *   1. Durable Orchestrator インスタンスへ外部イベントを注入 (raiseEvent)
 *   2. Logic App HTTP trigger を呼び出し
 *
 * 必要な環境変数:
 *   KAFKA_BOOTSTRAP_SERVERS       - Kafka ブートストラップサーバー
 *   LOGIC_APP_INV_RESERVED_URL    - la-ketana-ext2-inv-reserved の HTTP trigger URL
 *   LOGIC_APP_INV_FAILED_URL      - la-ketana-ext2-inv-failed の HTTP trigger URL
 *   LOGIC_APP_SHIP_SCHED_URL      - la-ketana-ext2-ship-sched の HTTP trigger URL
 */
public class KafkaConsumerFunctions {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    // -------------------------------------------------------------------------
    // inventory.reserved → InventoryResult (success) + Logic App
    // -------------------------------------------------------------------------
    @FunctionName("InventoryReservedConsumer")
    public void inventoryReservedConsumer(
            @KafkaTrigger(
                    name = "kafkaTrigger",
                    topic = "inventory.reserved",
                    brokerList = "%KAFKA_BOOTSTRAP_SERVERS%",
                    consumerGroup = "saga-orchestrator",
                    dataType = "string",
                    cardinality = Cardinality.ONE)
            String message,
            @DurableClientInput(name = "durableClient")
            DurableClientContext durableContext,
            ExecutionContext context) throws Exception {

        String inventoryReservedJson = extractValue(message);
        InventoryReservedMsg msg = MAPPER.readValue(inventoryReservedJson, InventoryReservedMsg.class);

        // 1. Durable Orchestrator へ外部イベント注入
        DurableTaskClient client = durableContext.getClient();
        InventoryResultEvent event = new InventoryResultEvent(true, null);
        client.raiseEvent(msg.orchestrationId(), "InventoryResult", event);
        context.getLogger().info("InventoryResult(success) raised: instanceId="
                + msg.orchestrationId() + " orderId=" + msg.orderId());

        // 2. Logic App HTTP trigger を呼び出し
        String logicAppUrl = System.getenv("LOGIC_APP_INV_RESERVED_URL");
        if (logicAppUrl != null && !logicAppUrl.isBlank()) {
            postToLogicApp(logicAppUrl, inventoryReservedJson, context);
        }
    }

    // -------------------------------------------------------------------------
    // inventory.reservation.failed → InventoryResult (failure) + Logic App
    // -------------------------------------------------------------------------
    @FunctionName("InventoryReservationFailedConsumer")
    public void inventoryReservationFailedConsumer(
            @KafkaTrigger(
                    name = "kafkaTrigger",
                    topic = "inventory.reservation.failed",
                    brokerList = "%KAFKA_BOOTSTRAP_SERVERS%",
                    consumerGroup = "saga-orchestrator",
                    dataType = "string",
                    cardinality = Cardinality.ONE)
            String message,
            @DurableClientInput(name = "durableClient")
            DurableClientContext durableContext,
            ExecutionContext context) throws Exception {

        String json = extractValue(message);
        InventoryReservationFailedMsg msg =
                MAPPER.readValue(json, InventoryReservationFailedMsg.class);

        // 1. Durable Orchestrator へ外部イベント注入
        DurableTaskClient client = durableContext.getClient();
        InventoryResultEvent event = new InventoryResultEvent(false, msg.reason());
        client.raiseEvent(msg.orchestrationId(), "InventoryResult", event);
        context.getLogger().info("InventoryResult(failed) raised: instanceId="
                + msg.orchestrationId() + " reason=" + msg.reason());

        // 2. Logic App HTTP trigger を呼び出し
        String logicAppUrl = System.getenv("LOGIC_APP_INV_FAILED_URL");
        if (logicAppUrl != null && !logicAppUrl.isBlank()) {
            postToLogicApp(logicAppUrl, json, context);
        }
    }

    // -------------------------------------------------------------------------
    // shipping.scheduled → ShippingResult + Logic App
    // -------------------------------------------------------------------------
    @FunctionName("ShippingScheduledConsumer")
    public void shippingScheduledConsumer(
            @KafkaTrigger(
                    name = "kafkaTrigger",
                    topic = "shipping.scheduled",
                    brokerList = "%KAFKA_BOOTSTRAP_SERVERS%",
                    consumerGroup = "saga-orchestrator",
                    dataType = "string",
                    cardinality = Cardinality.ONE)
            String message,
            @DurableClientInput(name = "durableClient")
            DurableClientContext durableContext,
            ExecutionContext context) throws Exception {

        String json = extractValue(message);
        ShippingScheduledMsg msg = MAPPER.readValue(json, ShippingScheduledMsg.class);

        // 1. Durable Orchestrator へ外部イベント注入
        DurableTaskClient client = durableContext.getClient();
        ShippingResultEvent event = new ShippingResultEvent(true, msg.shippingId());
        client.raiseEvent(msg.orchestrationId(), "ShippingResult", event);
        context.getLogger().info("ShippingResult raised: instanceId="
                + msg.orchestrationId() + " shippingId=" + msg.shippingId());

        // 2. Logic App HTTP trigger を呼び出し
        String logicAppUrl = System.getenv("LOGIC_APP_SHIP_SCHED_URL");
        if (logicAppUrl != null && !logicAppUrl.isBlank()) {
            postToLogicApp(logicAppUrl, json, context);
        }
    }

    // -------------------------------------------------------------------------
    // ヘルパー
    // -------------------------------------------------------------------------

    /**
     * Logic App HTTP trigger URL に JSON をそのまま POST する。
     */
    private static void postToLogicApp(String url, String jsonBody, ExecutionContext context) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();

        HttpResponse<String> resp = HTTP_CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
        int status = resp.statusCode();
        if (status < 200 || status >= 300) {
            throw new RuntimeException("Logic App HTTP trigger returned " + status + ": " + resp.body());
        }
        context.getLogger().fine("Logic App response: " + status);
    }

    /**
     * KafkaTrigger (cardinality=ONE, String) が渡す JSON から実際のメッセージ値を取り出す。
     * ランタイムが {"Offset":..., "Value":"...", "Topic":"..."} 形式で渡す場合に対応する。
     */
    private static String extractValue(String raw) throws Exception {
        JsonNode node = MAPPER.readTree(raw);
        for (String key : new String[]{"Value", "value"}) {
            JsonNode valueNode = node.get(key);
            if (valueNode != null) {
                return valueNode.isTextual() ? valueNode.asText() : valueNode.toString();
            }
        }
        return raw;
    }
}

