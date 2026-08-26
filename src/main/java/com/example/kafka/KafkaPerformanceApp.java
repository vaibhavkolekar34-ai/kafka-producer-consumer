package com.example.kafka;

public final class KafkaPerformanceApp {
    private KafkaPerformanceApp() {
    }

    public static void main(String[] args) {
        if (args.length == 0 || !("producer".equalsIgnoreCase(args[0]) || "consumer".equalsIgnoreCase(args[0]))) {
            System.err.println("Usage: java -jar kafka-producer-consumer-1.0.0.jar <producer|consumer> [config-path]");
            System.exit(1);
        }

        try {
            ApplicationConfig config = ApplicationConfig.load(args);
            String mode = args[0].toLowerCase();
            if ("producer".equals(mode)) {
                KafkaProducerApp.run(config);
            } else {
                KafkaConsumerApp.run(config);
            }
        } catch (Exception exception) {
            System.err.println("Application failed: " + exception.getMessage());
            exception.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
