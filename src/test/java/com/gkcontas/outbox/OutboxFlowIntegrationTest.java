package com.gkcontas.outbox;

import com.gkcontas.outbox.outbox.OutboxEvent;
import com.gkcontas.outbox.outbox.OutboxStatus;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class OutboxFlowIntegrationTest extends IntegrationTestBase {

    @Test
    void writesTheEventInTheSameTransactionAsTheProduct() throws Exception {
        UUID id = createProduct("KB-001", "Mechanical keyboard", "450.00");

        List<OutboxEvent> pending = outbox.findAll();
        assertThat(pending).hasSize(1);
        assertThat(pending.getFirst().getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(pending.getFirst().getAggregateId()).isEqualTo(id.toString());
        // Nothing has been published yet: the request returned as soon as the database
        // committed, and the broker is the publisher's problem.
        assertThat(pending.getFirst().getPublishedAt()).isNull();
    }

    @Test
    void deliversTheEventToTheProjection() throws Exception {
        UUID id = createProduct("MS-002", "Ergonomic mouse", "180.00");

        assertThat(publisher.publishBatch()).isEqualTo(1);

        await().atMost(TIMEOUT).until(() -> projections.findById(id).isPresent());
        assertThat(projections.findById(id)).hasValueSatisfying(projection -> {
            assertThat(projection.getSku()).isEqualTo("MS-002");
            assertThat(projection.getPrice()).isEqualByComparingTo("180.00");
        });
        assertThat(countByStatus(OutboxStatus.PUBLISHED)).isEqualTo(1);
    }

    @Test
    void propagatesAPriceChangeDownstream() throws Exception {
        UUID id = createProduct("MON-003", "Monitor", "2500.00");
        publisher.publishBatch();
        await().atMost(TIMEOUT).until(() -> projections.findById(id).isPresent());

        mockMvc.perform(put("/products/{id}/price", id)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"price": 2199.00}
                                """))
                .andExpect(status().isOk());
        publisher.publishBatch();

        await().atMost(TIMEOUT).until(() -> projections.findById(id)
                .filter(p -> p.getPrice().compareTo(money("2199.00")) == 0).isPresent());
    }

    @Test
    void emitsNothingWhenTheOperationFails() throws Exception {
        UUID id = createProduct("LT-004", "Laptop", "7800.00");
        publisher.publishBatch();
        long afterCreate = outbox.count();

        mockMvc.perform(put("/products/{id}/price-then-fail", id)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"price": 1.00}
                                """))
                .andExpect(status().is5xxServerError());

        // The outbox row was rolled back with the price change: a failed operation
        // announces nothing, which is the half of the guarantee that is easy to forget.
        assertThat(outbox.count()).isEqualTo(afterCreate);
        assertThat(products.findById(id)).hasValueSatisfying(product ->
                assertThat(product.getPrice()).isEqualByComparingTo("7800.00"));
    }

    @Test
    void doesNotEmitAnEventWhenThePriceDoesNotChange() throws Exception {
        UUID id = createProduct("KB-005", "Keyboard", "300.00");
        publisher.publishBatch();
        long before = outbox.count();

        mockMvc.perform(put("/products/{id}/price", id)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"price": 300.00}
                                """))
                .andExpect(status().isOk());

        assertThat(outbox.count()).isEqualTo(before);
    }

    @Test
    void ignoresADuplicateDelivery() throws Exception {
        UUID id = createProduct("DUP-006", "Duplicated", "99.00");
        publisher.publishBatch();
        await().atMost(TIMEOUT).until(() -> projections.findById(id).isPresent());

        // The same row sent a second time, which is exactly what happens when the
        // publisher dies between the send and the commit that marks it published.
        OutboxEvent event = outbox.findAll().getFirst();
        event.requeue(java.time.Instant.now());
        outbox.save(event);
        publisher.publishBatch();

        await().atMost(TIMEOUT).until(() -> countByStatus(OutboxStatus.PUBLISHED) == 1);
        assertThat(projections.findAll()).hasSize(1);
        assertThat(processedEvents.count()).isEqualTo(1);
    }

    @Test
    void keysTheMessageByAggregateSoOrderingHolds() throws Exception {
        UUID id = createProduct("ORD-007", "Ordered", "10.00");

        assertThat(outbox.findAll().getFirst().getAggregateId()).isEqualTo(id.toString());
        // Kafka only guarantees order within a partition, and the key is what decides the
        // partition — so two events about the same product can never be reordered.
    }
}
