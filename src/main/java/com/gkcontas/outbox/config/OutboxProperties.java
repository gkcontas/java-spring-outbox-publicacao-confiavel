package com.gkcontas.outbox.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.outbox")
public record OutboxProperties(
        String topic,
        int batchSize,
        int maxAttempts,
        Duration initialBackoff,
        Duration retention,
        boolean publisherEnabled) {

    public OutboxProperties {
        topic = topic == null ? "catalog-events" : topic;
        batchSize = batchSize <= 0 ? 100 : batchSize;
        maxAttempts = maxAttempts <= 0 ? 5 : maxAttempts;
        initialBackoff = initialBackoff == null ? Duration.ofSeconds(2) : initialBackoff;
        retention = retention == null ? Duration.ofDays(7) : retention;
    }
}
