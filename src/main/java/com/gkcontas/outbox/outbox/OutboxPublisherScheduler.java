package com.gkcontas.outbox.outbox;

import com.gkcontas.outbox.config.OutboxProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * The timer that drives the publisher.
 *
 * <p>Separated from {@link OutboxPublisher} so the publishing logic can be called directly
 * from a test, in the exact moment the test wants, instead of being waited for. It also
 * keeps the scheduling concern out of a class that already has enough to do.
 *
 * <p>{@code fixedDelay} rather than {@code fixedRate}: the interval counts from the end of
 * the previous run, so a slow batch does not queue another one behind it.
 */
@Component
@ConditionalOnProperty(name = "app.outbox.publisher-enabled", havingValue = "true",
        matchIfMissing = true)
class OutboxPublisherScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherScheduler.class);

    private final OutboxPublisher publisher;

    OutboxPublisherScheduler(OutboxPublisher publisher, OutboxProperties properties) {
        this.publisher = publisher;
        log.info("Outbox publisher active, topic {}", properties.topic());
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval:PT1S}")
    void publish() {
        int published = publisher.publishBatch();
        if (published > 0) {
            log.info("Published {} outbox events", published);
        }
    }

    @Scheduled(fixedDelayString = "${app.outbox.purge-interval:PT1H}")
    void purge() {
        int removed = publisher.purgePublished();
        if (removed > 0) {
            log.info("Purged {} delivered outbox events", removed);
        }
    }
}
