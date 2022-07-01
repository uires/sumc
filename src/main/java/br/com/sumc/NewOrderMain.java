package br.com.sumc;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties());
        var newOrder = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", UUID.randomUUID().toString(), "7878,1500");
        var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL",
                UUID.randomUUID().toString(), "We are processing your order.");

        producer.send(newOrder, handleProduceCallback()).get();
        producer.send(emailRecord, handleProduceCallback()).get();
    }

    private static Callback handleProduceCallback() {
        return (data, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
                return;
            }

            System.out.println("Topic sent " + data.topic() + ":::partition "
                    + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
