package br.com.sumc;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) {
        var logService = new LogService();
        try (var service = new KafkaService(LogService.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"),
                logService::parse, Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("LOG --------------------------------");
        System.out.println("key:: " + record.key());
        System.out.println("key:: " + record.value());
        System.out.println("--------------------------------");
    }
}
