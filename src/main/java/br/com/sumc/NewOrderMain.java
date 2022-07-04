package br.com.sumc;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var dispatcher = new KafkaDispatcher()) {
            var key = UUID.randomUUID().toString();
            dispatcher.send("ECOMMERCE_NEW_ORDER", key, "123,15447");
            dispatcher.send("ECOMMERCE_SEND_EMAIL", key, "We are processing your order.");
        }
    }

}
