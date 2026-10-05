package com.gkcontas.outbox.api;

import com.gkcontas.outbox.outbox.OutboxEvent;
import com.gkcontas.outbox.outbox.OutboxPublisher;
import com.gkcontas.outbox.outbox.OutboxRepository;
import com.gkcontas.outbox.outbox.OutboxStatus;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.data.domain.Limit;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Makes the outbox visible.
 *
 * <p>An outbox nobody can look at is a queue with no monitoring: the first sign of trouble
 * becomes a downstream team asking why their data is stale. The count of pending rows is
 * the single most useful metric this pattern produces — if it climbs and does not come
 * back down, the broker or the publisher is in trouble.
 */
@RestController
@RequestMapping("/outbox")
class OutboxController {

    private final OutboxRepository repository;
    private final OutboxPublisher publisher;

    OutboxController(OutboxRepository repository, OutboxPublisher publisher) {
        this.repository = repository;
        this.publisher = publisher;
    }

    record OutboxView(UUID id, String aggregateId, String eventType, OutboxStatus status,
                      int attempts, String lastError, Instant createdAt, Instant publishedAt) {

        static OutboxView from(OutboxEvent event) {
            return new OutboxView(event.getId(), event.getAggregateId(), event.getEventType(),
                    event.getStatus(), event.getAttempts(), event.getLastError(),
                    event.getCreatedAt(), event.getPublishedAt());
        }
    }

    @GetMapping
    List<OutboxView> byStatus(@RequestParam(defaultValue = "PENDING") OutboxStatus status,
                              @RequestParam(defaultValue = "50") int limit) {
        return repository.findByStatusOrderByCreatedAtDesc(status, Limit.of(limit)).stream()
                .map(OutboxView::from)
                .toList();
    }

    @GetMapping("/summary")
    Map<String, Long> summary() {
        return Map.of(
                "pending", repository.countByStatus(OutboxStatus.PENDING),
                "published", repository.countByStatus(OutboxStatus.PUBLISHED),
                "failed", repository.countByStatus(OutboxStatus.FAILED));
    }

    /** Moves every parked event back to pending, for an operator to retry after a fix. */
    @PostMapping("/retry-failed")
    Map<String, Integer> retryFailed() {
        return Map.of("requeued", publisher.requeueFailed());
    }

    /** Runs one batch immediately, instead of waiting for the next tick. */
    @PostMapping("/flush")
    Map<String, Integer> flush() {
        return Map.of("published", publisher.publishBatch());
    }
}
