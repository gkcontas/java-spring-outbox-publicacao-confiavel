package com.gkcontas.outbox;

import com.gkcontas.outbox.catalog.ProductEvents;
import com.gkcontas.outbox.outbox.OutboxRecorder;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The constraint that keeps the pattern honest.
 *
 * <p>{@code Propagation.MANDATORY} on the recorder means an event can only be written
 * inside an existing transaction — the caller's. Someone who records an event from a place
 * with no transaction gets an exception here rather than a system that has quietly gone
 * back to being a dual write.
 */
class OutboxRecorderIntegrationTest extends IntegrationTestBase {

    @Autowired
    private OutboxRecorder recorder;

    @Autowired
    private TransactionTemplate transactionTemplate;

    private ProductEvents.ProductCreated anEvent() {
        return new ProductEvents.ProductCreated(UUID.randomUUID(), "REC-01", "Recorded",
                money("10.00"));
    }

    @Test
    void refusesToRecordOutsideATransaction() {
        assertThatThrownBy(() -> recorder.record("product", "any", anEvent()))
                .isInstanceOf(IllegalTransactionStateException.class);

        assertThat(outbox.count()).isZero();
    }

    @Test
    void storesTheEventTypeAndTheSerialisedPayload() {
        var event = anEvent();

        transactionTemplate.executeWithoutResult(status ->
                recorder.record("product", event.productId().toString(), event));

        assertThat(outbox.findAll()).singleElement().satisfies(stored -> {
            assertThat(stored.getEventType()).isEqualTo("ProductCreated");
            assertThat(stored.getAggregateType()).isEqualTo("product");
            assertThat(stored.getAggregateId()).isEqualTo(event.productId().toString());
            assertThat(stored.getPayload()).contains("\"sku\":\"REC-01\"");
            assertThat(stored.getAttempts()).isZero();
        });
    }

    @Test
    void discardsTheEventWhenTheSurroundingTransactionRollsBack() {
        var event = anEvent();

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            recorder.record("product", event.productId().toString(), event);
            throw new IllegalStateException("something went wrong afterwards");
        })).isInstanceOf(IllegalStateException.class);

        // The event shares the fate of the change it describes, which is the entire point.
        assertThat(outbox.count()).isZero();
    }
}
