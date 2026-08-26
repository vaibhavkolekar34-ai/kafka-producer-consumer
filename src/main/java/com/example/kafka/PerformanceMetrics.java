package com.example.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class PerformanceMetrics {
    private final List<Long> latenciesMicros = new ArrayList<>();

    void recordLatencyMillis(long latencyMillis) {
        latenciesMicros.add(Math.max(0, latencyMillis) * 1_000);
    }

    int messageCount() {
        return latenciesMicros.size();
    }

    double averageLatencyMs() {
        return latenciesMicros.stream().mapToLong(Long::longValue).average().orElse(0.0) / 1_000.0;
    }

    double percentileLatencyMs(double percentile) {
        if (latenciesMicros.isEmpty()) {
            return 0.0;
        }
        List<Long> sorted = new ArrayList<>(latenciesMicros);
        Collections.sort(sorted);
        int index = (int) Math.ceil(percentile * sorted.size()) - 1;
        return sorted.get(Math.max(0, index)) / 1_000.0;
    }

    double maximumLatencyMs() {
        return latenciesMicros.stream().mapToLong(Long::longValue).max().orElse(0L) / 1_000.0;
    }
}
