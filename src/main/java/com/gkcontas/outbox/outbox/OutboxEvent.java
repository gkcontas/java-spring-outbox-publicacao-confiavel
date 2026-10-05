package com.gkcontas.outbox.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * One row per event waiting to leave the service.
 *
 * <p>{@code aggregateId} becomes the message key, and therefore the partition. That is the
 * only ordering Kafka offers: events for the same product stay in order relative to each
 * other, and events for different products do not — which is what any sane consumer
 * expects anyway.
 *
 * <p>{@code id} travels with the message and is what the consumer deduplicates on.
 * Delivery here is at-least-once by construction: the publisher can succeed in sending and
 * then die before marking the row, and the next run will send it again.
 */
@Entity
@Table(name = "outbox_event")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEvent {

    @Id
    private UUID id;

    @Column(name = "aggregate_type", nullable = false, length = 40)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false, length = 64)
    private String aggregateId;

    @Column(name = "event_type", nullable = false, length = 60)
    private String eventType;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private OutboxStatus status;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "last_error", columnDefinition = "text")
    private String lastError;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "next_attempt_at", nullable = false)
    private Instant nextAttemptAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    public static OutboxEvent pending(String aggregateType, String aggregateId,
                                      String eventType, String payload, Instant now) {
        return new OutboxEvent(UUID.randomUUID(), aggregateType, aggregateId, eventType, payload,
                OutboxStatus.PENDING, 0, null, now, now, null);
    }

    /**
     * Puts a parked event back in the queue.
     *
     * <p>{@code FAILED} means "stopped trying", never "discarded" — the row and its
     * payload are still there. Requeueing is an operator decision, because whatever made
     * it fail five times in a row is unlikely to have fixed itself.
     */
    public void requeue(Instant now) {
        status = OutboxStatus.PENDING;
        attempts = 0;
        nextAttemptAt = now;
    }

    public void markPublished(Instant now) {
        status = OutboxStatus.PUBLISHED;
        publishedAt = now;
        lastError = null;
    }

    /**
     * Records a failed attempt and schedules the next one.
     *
     * <p>Backoff matters more here than in a request path: the publisher runs on a timer,
     * so without it a broker that is down turns into a tight retry loop against every
     * pending row, every second, for as long as the outage lasts.
     */
    public void markFailure(String error, int maxAttempts, Instant now, java.time.Duration backoff) {
        attempts++;
        lastError = error;
        if (attempts >= maxAttempts) {
            // A row nobody can publish must stop being tried, or it blocks the batch it
            // sits in and hides every event queued behind it.
            status = OutboxStatus.FAILED;
        } else {
            nextAttemptAt = now.plus(backoff.multipliedBy(1L << (attempts - 1)));
        }
    }
}
