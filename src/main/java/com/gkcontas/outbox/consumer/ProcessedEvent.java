package com.gkcontas.outbox.consumer;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * The consumer's memory of what it has already handled.
 *
 * <p>At-least-once delivery is not a shortcoming of this implementation; it is the best
 * any outbox can offer, because the send and the "mark as sent" cannot be atomic. So the
 * duplicate is a certainty, not a risk, and the consumer is where it stops.
 */
@Entity
@Table(name = "processed_event")
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedEvent {

    @Id
    @Column(name = "event_id")
    private UUID eventId;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;
}
