package com.example.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

final class ApplicationConfig {
    private final Properties properties;

    private ApplicationConfig(Properties properties) {
        this.properties = properties;
    }

    static ApplicationConfig load(String[] args) throws IOException {
        Path configPath = args.length > 1
                ? Path.of(args[1])
                : Path.of("config", "application.properties");

        if (!Files.isRegularFile(configPath)) {
            throw new IllegalArgumentException("Configuration file not found: " + configPath.toAbsolutePath());
        }

        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(configPath)) {
            properties.load(input);
        }
        return new ApplicationConfig(properties);
    }

    String required(String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required configuration: " + key);
        }
        return value.trim();
    }

    int getInt(String key) {
        try {
            return Integer.parseInt(required(key));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Configuration " + key + " must be an integer.", exception);
        }
    }

    String get(String key) {
        return required(key);
    }
}
