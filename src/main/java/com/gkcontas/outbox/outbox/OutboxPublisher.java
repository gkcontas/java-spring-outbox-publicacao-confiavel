package com.gkcontas.outbox.outbox;

import com.gkcontas.outbox.config.OutboxProperties;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Moves the outbox to the broker, one batch at a time.
 *
 * <p>The ordering inside the method is deliberate and is where most home-grown outboxes go
 * wrong. The rows are claimed with a lock, sent, and only then marked — all inside one
 * transaction. If the send fails, the transaction rolls back the attempt counter along
 * with everything else and the rows stay pending; if the process dies after the send and
 * before the commit, the rows stay pending and are sent again. The second case is why
 * delivery is <em>at least</em> once and never exactly once, and why the consumer carries
 * a deduplication table.
 *
 * <p>Sending synchronously ({@code get}) rather than fire-and-forget is also deliberate: an
 * asynchronous send would let the transaction commit "published" for messages the broker
 * never acknowledged.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxProperties properties;
    private final Clock clock;

    public OutboxPublisher(OutboxRepository repository,
                           KafkaTemplate<String, String> kafkaTemplate,
                           OutboxProperties properties,
                           Clock clock) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * Publishes one batch and reports how many rows left.
     *
     * @return the number of events actually delivered
     */
    @Transactional
    public int publishBatch() {
        List<OutboxEvent> batch = repository.claimBatch(clock.instant(),
                Limit.of(properties.batchSize()));
        if (batch.isEmpty()) {
            return 0;
        }

        int published = 0;
        for (OutboxEvent event : batch) {
            if (send(event)) {
                published++;
            }
        }
        return published;
    }

    private boolean send(OutboxEvent event) {
        try {
            kafkaTemplate.send(MessageBuilder
                            .withPayload(event.getPayload())
                            .setHeader(KafkaHeaders.TOPIC, properties.topic())
                            // The aggregate id as key: same product, same partition, and
                            // therefore the only ordering guarantee Kafka can give.
                            .setHeader(KafkaHeaders.KEY, event.getAggregateId())
                            // The event id travels in a header so the consumer can
                            // deduplicate without parsing the payload.
                            .setHeader("event-id", event.getId().toString())
                            .setHeader("event-type", event.getEventType())
                            .build())
                    .get(10, TimeUnit.SECONDS);

            event.markPublished(clock.instant());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing " + event.getId(), e);
        } catch (Exception e) {
            String reason = e.getCause() == null ? e.toString() : e.getCause().toString();
            event.markFailure(reason, properties.maxAttempts(), clock.instant(),
                    properties.initialBackoff());
            log.warn("Could not publish outbox event {} (attempt {}): {}",
                    event.getId(), event.getAttempts(), reason);
            return false;
        }
    }

    /**
     * Moves parked events back to pending.
     *
     * <p>Worth knowing before pressing it: a publish that timed out may well have been
     * delivered — the broker can accept the record and fail to acknowledge it in time. So
     * requeueing often produces a duplicate, which is harmless here only because the
     * consumer deduplicates by event id.
     */
    @Transactional
    public int requeueFailed() {
        List<OutboxEvent> failed = repository.findByStatusOrderByCreatedAtDesc(
                OutboxStatus.FAILED, Limit.of(properties.batchSize()));
        failed.forEach(event -> event.requeue(clock.instant()));
        return failed.size();
    }

    @Transactional
    public int purgePublished() {
        return repository.deletePublishedBefore(clock.instant().minus(properties.retention()));
    }
}
