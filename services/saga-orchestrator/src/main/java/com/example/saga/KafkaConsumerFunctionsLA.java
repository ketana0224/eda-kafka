package com.example.saga;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.Cardinality;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.KafkaTrigger;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * Logic Apps Consumption 向け Kafka ブリッジ（3.LogicApp.md 用）。
 *
 * Durable Functions には依存しない。
 * Kafka トピックをポーリングし、対応する Logic App Consumer の HTTP trigger を POST するだけ。
 *
 * func-ketana-ext2-kafka-bridge にデプロイして使用する。
 * func-ketana-ext2-saga-orch にも同じパッケージからデプロイされるが、
 * そちらでは LOGIC_APP_*_URL が未設定のため何もしない（コンシューマーグループ saga-la-bridge で独立）。
 *
 * 必要な環境変数:
 *   KAFKA_BOOTSTRAP_SERVERS       - Kafka ブートストラップサーバー
 *   LOGIC_APP_INV_RESERVED_URL    - la-ketana-ext2-inv-reserved の HTTP trigger URL
 *   LOGIC_APP_INV_FAILED_URL      - la-ketana-ext2-inv-failed の HTTP trigger URL
 *   LOGIC_APP_SHIP_SCHED_URL      - la-ketana-ext2-ship-sched の HTTP trigger URL
 */
public class KafkaConsumerFunctionsLA {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    // -------------------------------------------------------------------------
    // inventory.reserved → la-ketana-ext2-inv-reserved
    // -------------------------------------------------------------------------
    @FunctionName("InventoryReservedBridge")
    public void inventoryReservedBridge(
            @KafkaTrigger(
                    name = "kafkaTrigger",
                    topic = "inventory.reserved",
                    brokerList = "%KAFKA_BOOTSTRAP_SERVERS%",
                    consumerGroup = "saga-la-bridge",
                    dataType = "string",
                    cardinality = Cardinality.ONE)
            String message,
            ExecutionContext context) throws Exception {

        String json = extractValue(message);
        String logicAppUrl = System.getenv("LOGIC_APP_INV_RESERVED_URL");
        if (logicAppUrl == null || logicAppUrl.isBlank()) {
            context.getLogger().fine("LOGIC_APP_INV_RESERVED_URL not set, skipping.");
            return;
        }
        postToLogicApp(logicAppUrl, json, context);
        context.getLogger().info("InventoryReservedBridge: forwarded to Logic App");
    }

    // -------------------------------------------------------------------------
    // inventory.reservation.failed → la-ketana-ext2-inv-failed
    // -------------------------------------------------------------------------
    @FunctionName("InventoryReservationFailedBridge")
    public void inventoryReservationFailedBridge(
            @KafkaTrigger(
                    name = "kafkaTrigger",
                    topic = "inventory.reservation.failed",
                    brokerList = "%KAFKA_BOOTSTRAP_SERVERS%",
                    consumerGroup = "saga-la-bridge",
                    dataType = "string",
                    cardinality = Cardinality.ONE)
            String message,
            ExecutionContext context) throws Exception {

        String json = extractValue(message);
        String logicAppUrl = System.getenv("LOGIC_APP_INV_FAILED_URL");
        if (logicAppUrl == null || logicAppUrl.isBlank()) {
            context.getLogger().fine("LOGIC_APP_INV_FAILED_URL not set, skipping.");
            return;
        }
        postToLogicApp(logicAppUrl, json, context);
        context.getLogger().info("InventoryReservationFailedBridge: forwarded to Logic App");
    }

    // -------------------------------------------------------------------------
    // shipping.scheduled → la-ketana-ext2-ship-sched
    // -------------------------------------------------------------------------
    @FunctionName("ShippingScheduledBridge")
    public void shippingScheduledBridge(
            @KafkaTrigger(
                    name = "kafkaTrigger",
                    topic = "shipping.scheduled",
                    brokerList = "%KAFKA_BOOTSTRAP_SERVERS%",
                    consumerGroup = "saga-la-bridge",
                    dataType = "string",
                    cardinality = Cardinality.ONE)
            String message,
            ExecutionContext context) throws Exception {

        String json = extractValue(message);
        String logicAppUrl = System.getenv("LOGIC_APP_SHIP_SCHED_URL");
        if (logicAppUrl == null || logicAppUrl.isBlank()) {
            context.getLogger().fine("LOGIC_APP_SHIP_SCHED_URL not set, skipping.");
            return;
        }
        postToLogicApp(logicAppUrl, json, context);
        context.getLogger().info("ShippingScheduledBridge: forwarded to Logic App");
    }

    // -------------------------------------------------------------------------
    // ヘルパー
    // -------------------------------------------------------------------------

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
