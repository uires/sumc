package br.com.sumc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

class KafkaService implements Closeable {
    private final KafkaConsumer<String, String> consumer;
    private final ConsumerFunction parse;

    public KafkaService(String groupId, String topic, ConsumerFunction parse, Map<String, String> properties) {
        this(parse, groupId, properties);
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction parse, Map<String, String> properties) {
        this(parse, groupId, properties);
        this.consumer.subscribe(topic);
    }

    private KafkaService(ConsumerFunction parse, String groupId, Map<String, String> properties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(groupId, properties));
    }

    void run() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            verifyRecordsToProcess(records);
        }
    }

    private void processPoll(ConsumerRecords<String, String> records) {
        for (var record : records) {
            this.parse.consume(record);
        }
    }

    private void verifyRecordsToProcess(ConsumerRecords<String, String> records) {
        if (!records.isEmpty()) {
            processPoll(records);
        }
    }

    private static Properties getProperties(String groupId, Map<String, String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.putAll(overrideProperties);

        return properties;
    }

    @Override
    public void close() {
        this.consumer.close();
    }
}
