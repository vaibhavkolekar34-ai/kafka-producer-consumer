package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

final class KafkaProducerApp {
    private KafkaProducerApp() {
    }

    static void run(ApplicationConfig config) throws Exception {
        String topic = config.get("kafka.topic");
        String runId = config.get("test.run.id");
        int messageCount = config.getInt("test.message.count");

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get("kafka.bootstrap.servers"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, config.get("producer.acks"));
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getInt("producer.batch.size"));
        properties.put(ProducerConfig.LINGER_MS_CONFIG, config.getInt("producer.linger.ms"));
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.get("producer.compression.type"));
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        System.out.printf("Producer started: runId=%s, topic=%s, messages=%d%n", runId, topic, messageCount);
        long startNanos = System.nanoTime();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int sequence = 1; sequence <= messageCount; sequence++) {
                long sentAtMillis = System.currentTimeMillis();
                String value = runId + "|" + sequence + "|" + sentAtMillis;
                producer.send(new ProducerRecord<>(topic, runId, value));
            }
            producer.flush();
        }
        long elapsedNanos = System.nanoTime() - startNanos;
        double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
        double throughput = messageCount / elapsedSeconds;
        System.out.printf("Producer completed: %,d messages in %.3f s (%.2f messages/s)%n",
                messageCount, elapsedSeconds, throughput);
    }
}
