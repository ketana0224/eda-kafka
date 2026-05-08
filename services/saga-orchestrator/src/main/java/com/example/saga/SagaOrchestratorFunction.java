package com.example.saga;

import com.example.saga.model.*;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.durabletask.Task;
import com.microsoft.durabletask.TaskOrchestrationContext;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

/**
 * Saga Orchestrator。
 *
 * 正常フロー:
 *   ① CreateOrder Activity         → OrderService に HTTP POST、orderId を取得
 *   ② PublishInventoryReserveCommand Activity  → inventory.reserve.command 発行
 *   ③ waitForExternalEvent("InventoryResult") / 30 分タイムアウト
 *   ④ PublishShippingScheduleCommand Activity  → shipping.schedule.command 発行
 *   ⑤ waitForExternalEvent("ShippingResult") / 30 分タイムアウト
 *   ⑥ PublishOrderConfirmed Activity           → order.confirmed 発行
 *
 * 補償フロー（在庫不足 / タイムアウト）:
 *   ⑦ PublishInventoryRelease Activity         → inventory.release.command 発行（引当済みの場合）
 *   ⑧ PublishOrderCancelled Activity           → order.cancelled 発行
 */
public class SagaOrchestratorFunction {

    private static final int TIMEOUT_MINUTES = Integer.parseInt(
            System.getenv().getOrDefault("SAGA_TIMEOUT_MINUTES", "30"));

    @FunctionName("OrderSagaOrchestrator")
    public void orderSagaOrchestrator(
            @DurableOrchestrationTrigger(name = "rpcResult")
            TaskOrchestrationContext ctx) {

        SagaInput input = ctx.getInput(SagaInput.class);
        String instanceId = ctx.getInstanceId();

        // ① Create Order（OrderService に HTTP POST）
        String orderId = ctx.callActivity("CreateOrder", input, String.class).await();

        // ② Reserve Inventory
        InventoryReserveCommand invCmd = new InventoryReserveCommand(
                orderId, input.items(), instanceId, Instant.now().toString());
        ctx.callActivity("PublishInventoryReserveCommand", invCmd).await();

        // ③ Wait for Inventory result
        Task<InventoryResultEvent> inventoryEventTask =
                ctx.waitForExternalEvent("InventoryResult", InventoryResultEvent.class);
        Task<Void> inventoryTimerTask =
                ctx.createTimer(ctx.getCurrentInstant().atZone(ZoneOffset.UTC)
                        .plus(Duration.ofMinutes(TIMEOUT_MINUTES)));

        Task<?> inventoryWinner = ctx.anyOf(inventoryEventTask, inventoryTimerTask).await();

        if (inventoryWinner == inventoryTimerTask) {
            // 在庫引当タイムアウト → 補償
            ctx.callActivity("PublishOrderCancelled",
                    new OrderCancelledCommand("OrderCancelled", orderId,
                            "InventoryTimeout", Instant.now().toString())).await();
            return;
        }

        InventoryResultEvent invResult = inventoryEventTask.await();
        if (!invResult.success()) {
            // 在庫不足 → 補償
            ctx.callActivity("PublishOrderCancelled",
                    new OrderCancelledCommand("OrderCancelled", orderId,
                            invResult.reason(), Instant.now().toString())).await();
            return;
        }

        // ④ Schedule Shipping
        ShippingScheduleCommand shpCmd = new ShippingScheduleCommand(
                orderId, input.shippingAddress(), instanceId, Instant.now().toString());
        ctx.callActivity("PublishShippingScheduleCommand", shpCmd).await();

        // ⑤ Wait for Shipping result
        Task<ShippingResultEvent> shippingEventTask =
                ctx.waitForExternalEvent("ShippingResult", ShippingResultEvent.class);
        Task<Void> shippingTimerTask =
                ctx.createTimer(ctx.getCurrentInstant().atZone(ZoneOffset.UTC)
                        .plus(Duration.ofMinutes(TIMEOUT_MINUTES)));

        Task<?> shippingWinner = ctx.anyOf(shippingEventTask, shippingTimerTask).await();

        if (shippingWinner == shippingTimerTask) {
            // 配送手配タイムアウト → 在庫解放 → 補償
            ctx.callActivity("PublishInventoryRelease",
                    new InventoryReleaseCommand(orderId, Instant.now().toString())).await();
            ctx.callActivity("PublishOrderCancelled",
                    new OrderCancelledCommand("OrderCancelled", orderId,
                            "ShippingTimeout", Instant.now().toString())).await();
            return;
        }

        ShippingResultEvent shpResult = shippingEventTask.await();

        // ⑥ Confirm Order
        ctx.callActivity("PublishOrderConfirmed",
                new OrderConfirmedCommand("OrderConfirmed", orderId,
                        shpResult.shippingId(), Instant.now().toString())).await();
    }
}
