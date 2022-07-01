package br.com.sumc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class FraudDetectorService {

    public static void main(String[] args) {
        Properties properties = properties();
        var consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            verifyRecordsToProcess(records);
        }
    }

    private static void verifyRecordsToProcess(ConsumerRecords<String, String> records) {
        if (!records.isEmpty()) {
            processPoll(records);
        }
    }

    private static void processPoll(ConsumerRecords<String, String> records) {
        for (var record : records) {
            System.out.println("--------------------------------");
            System.out.println("key:: " + record.key() + " value: "
                    + record.value() + " partition: " + record.partition()
                    + " offset: " + record.offset());
            System.out.println("--------------------------------");

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            System.out.println("Order processed");
        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());

        return properties;
    }

}
