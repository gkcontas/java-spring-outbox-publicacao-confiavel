package com.gkcontas.outbox.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Writes the event into the outbox — and does nothing else.
 *
 * <p>{@code Propagation.MANDATORY} is the whole design, expressed as a constraint: this
 * method refuses to run unless a transaction is already open. The caller is in the middle
 * of changing a product, so the row lands in the same transaction as the change, and the
 * two are committed or discarded together.
 *
 * <p>Without it, somebody eventually calls this from a place with no transaction, the
 * event is committed on its own, and the outbox silently goes back to being a dual write
 * with extra steps. {@code REQUIRED} would create that transaction and hide the mistake;
 * {@code MANDATORY} turns it into an exception at the first call.
 */
@Component
public class OutboxRecorder {

    private final OutboxRepository repository;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public OutboxRecorder(OutboxRepository repository, ObjectMapper objectMapper, Clock clock) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public OutboxEvent record(String aggregateType, String aggregateId, Object event) {
        return repository.save(OutboxEvent.pending(
                aggregateType,
                aggregateId,
                event.getClass().getSimpleName(),
                serialize(event),
                clock.instant()));
    }

    private String serialize(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            // Failing here rolls back the business change too, which is the right outcome:
            // an event that cannot be serialised would be a silent gap downstream.
            throw new IllegalStateException("Could not serialise " + event.getClass(), e);
        }
    }
}
