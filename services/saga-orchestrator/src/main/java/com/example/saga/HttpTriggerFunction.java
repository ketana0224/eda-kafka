package com.example.saga;

import com.example.saga.model.SagaInput;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.NewOrchestrationInstanceOptions;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;

import java.util.Optional;

/**
 * HTTP エントリポイント。
 * POST /api/orders を受信し、OrderSagaOrchestrator を起動する。
 * クライアントには 202 Accepted + Durable Functions 管理 URL を返す。
 */
public class HttpTriggerFunction {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @FunctionName("StartOrderSaga")
    public HttpResponseMessage startOrderSaga(
            @HttpTrigger(
                    name = "req",
                    methods = {HttpMethod.POST},
                    route = "orders",
                    authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableClient")
            DurableClientContext durableContext,
            ExecutionContext context) {

        String body = request.getBody().orElse("{}");

        SagaInput input;
        try {
            input = MAPPER.readValue(body, SagaInput.class);
        } catch (Exception e) {
            context.getLogger().warning("Invalid request body: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body("Invalid request body: " + e.getMessage())
                    .build();
        }

        if (input.customerId() == null || input.shippingAddress() == null
                || input.items() == null || input.items().isEmpty()) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body("customerId, shippingAddress, items are required")
                    .build();
        }

        DurableTaskClient client = durableContext.getClient();
        String instanceId;
        try {
            instanceId = client.scheduleNewOrchestrationInstance(
                    "OrderSagaOrchestrator",
                    new NewOrchestrationInstanceOptions().setInput(input));
        } catch (Exception e) {
            context.getLogger().severe("Failed to start orchestration: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to start orchestration")
                    .build();
        }

        context.getLogger().info("OrderSagaOrchestrator started: instanceId=" + instanceId);
        return durableContext.createCheckStatusResponse(request, instanceId);
    }
}
