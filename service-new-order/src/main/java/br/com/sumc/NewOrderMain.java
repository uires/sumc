package br.com.sumc;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            try (var dispatcherEmail = new KafkaDispatcher<String>()) {
                var userId = UUID.randomUUID().toString();
                var orderId = UUID.randomUUID().toString();
                var order = new Order(userId, orderId, new BigDecimal(Math.random() * 50000 + 1));

                orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);
                dispatcherEmail.send("ECOMMERCE_SEND_EMAIL", userId, "We are processing your order.");
            }
        }
    }

}
