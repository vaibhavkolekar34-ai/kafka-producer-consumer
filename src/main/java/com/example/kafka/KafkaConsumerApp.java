package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

final class KafkaConsumerApp {
    private KafkaConsumerApp() {
    }

    static void run(ApplicationConfig config) throws IOException {
        String topic = config.get("kafka.topic");
        String runId = config.get("test.run.id");
        int expectedCount = config.getInt("test.message.count");
        int maxWaitSeconds = config.getInt("consumer.max.wait.seconds");

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get("kafka.bootstrap.servers"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, config.get("consumer.group.id"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.get("consumer.auto.offset.reset"));
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.get("consumer.enable.auto.commit"));

        PerformanceMetrics metrics = new PerformanceMetrics();
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(maxWaitSeconds).toNanos();
        long firstMessageNanos = 0;
        long lastMessageNanos = 0;

        System.out.printf("Consumer connecting: runId=%s, topic=%s, expecting %,d messages%n", runId, topic, expectedCount);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singleton(topic));
            ConsumerRecords<String, String> assignmentRecords = ConsumerRecords.empty();
            while (consumer.assignment().isEmpty() && System.nanoTime() < deadlineNanos) {
                assignmentRecords = consumer.poll(Duration.ofMillis(config.getInt("consumer.poll.timeout.ms")));
            }
            if (consumer.assignment().isEmpty()) {
                throw new IllegalStateException("Consumer was not assigned a partition within " + maxWaitSeconds + " seconds.");
            }
            System.out.printf("Consumer ready: assigned partitions=%s%n", consumer.assignment());
            boolean processAssignmentRecords = true;
            while (metrics.messageCount() < expectedCount && System.nanoTime() < deadlineNanos) {
                ConsumerRecords<String, String> records = processAssignmentRecords
                        ? assignmentRecords
                        : consumer.poll(Duration.ofMillis(config.getInt("consumer.poll.timeout.ms")));
                processAssignmentRecords = false;
                for (var record : records) {
                    String[] fields = record.value().split("\\|", 3);
                    if (fields.length != 3 || !runId.equals(fields[0])) {
                        continue;
                    }
                    try {
                        long sentAtMillis = Long.parseLong(fields[2]);
                        long receivedAtNanos = System.nanoTime();
                        if (firstMessageNanos == 0) {
                            firstMessageNanos = receivedAtNanos;
                        }
                        lastMessageNanos = receivedAtNanos;
                        metrics.recordLatencyMillis(System.currentTimeMillis() - sentAtMillis);
                    } catch (NumberFormatException ignored) {
                        System.err.println("Skipped malformed message at " + record.topic() + "-" + record.partition() + " offset " + record.offset());
                    }
                    if (metrics.messageCount() == expectedCount) {
                        break;
                    }
                }
            }
            consumer.commitSync();
        }

        if (metrics.messageCount() != expectedCount) {
            throw new IllegalStateException("Timed out after " + maxWaitSeconds + " seconds. Received "
                    + metrics.messageCount() + " of " + expectedCount + " messages for run " + runId + ".");
        }

        double elapsedSeconds = (lastMessageNanos - firstMessageNanos) / 1_000_000_000.0;
        double throughput = elapsedSeconds == 0 ? expectedCount : expectedCount / elapsedSeconds;
        String csv = String.format("test,batchSize,lingerMs,compression,messages,throughput,avgLatencyMs,p50Ms,p95Ms,p99Ms,maxLatencyMs%n"
                        + "%s,%d,%d,%s,%d,%.2f,%.3f,%.3f,%.3f,%.3f,%.3f%n",
                runId, config.getInt("producer.batch.size"), config.getInt("producer.linger.ms"),
                config.get("producer.compression.type"), metrics.messageCount(), throughput,
                metrics.averageLatencyMs(), metrics.percentileLatencyMs(0.50), metrics.percentileLatencyMs(0.95),
                metrics.percentileLatencyMs(0.99), metrics.maximumLatencyMs());

        Path resultFile = Path.of(config.get("test.result.directory"), runId + ".csv");
        Files.createDirectories(resultFile.getParent());
        Files.writeString(resultFile, csv, StandardCharsets.UTF_8);
        System.out.println("Consumer completed successfully.");
        System.out.print(csv);
        System.out.println("Results written to " + resultFile.toAbsolutePath());
    }
}
