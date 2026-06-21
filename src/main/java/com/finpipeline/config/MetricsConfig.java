package com.finpipeline.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Centralized metrics configuration.
 *
 * Exposes named counters and timers via Micrometer.
 * Available at: GET /actuator/metrics/{metric-name}
 *
 * In production these feed into Datadog, Prometheus, CloudWatch, etc.
 * Here they are exposed via Actuator endpoints for demo purposes.
 */
@Configuration
public class MetricsConfig {

    // --- Counters ---

    @Bean
    public Counter processedCounter(MeterRegistry registry) {
        return Counter.builder("pipeline.transactions.processed")
                .description("Total transactions successfully processed")
                .register(registry);
    }

    @Bean
    public Counter failedCounter(MeterRegistry registry) {
        return Counter.builder("pipeline.transactions.failed")
                .description("Total transactions that failed processing")
                .register(registry);
    }

    @Bean
    public Counter duplicateCounter(MeterRegistry registry) {
        return Counter.builder("pipeline.transactions.duplicate")
                .description("Total transactions rejected as duplicates")
                .register(registry);
    }

    @Bean
    public Counter dlqCounter(MeterRegistry registry) {
        return Counter.builder("pipeline.transactions.dlq")
                .description("Total transactions routed to Dead Letter Queue")
                .register(registry);
    }

    @Bean
    public Counter timeoutCounter(MeterRegistry registry) {
        return Counter.builder("pipeline.publish.timeout")
                .description("Total async publish timeouts")
                .register(registry);
    }

    // --- Timer ---

    @Bean
    public Timer processingTimer(MeterRegistry registry) {
        return Timer.builder("pipeline.processing.duration")
                .description("End-to-end processing time per transaction")
                .register(registry);
    }
}