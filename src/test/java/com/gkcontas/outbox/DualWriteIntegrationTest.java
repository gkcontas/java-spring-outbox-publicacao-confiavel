package com.gkcontas.outbox;

import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The problem the pattern exists to solve, reproduced.
 *
 * <p>Without this test the outbox is a lot of machinery justified by a story. With it, the
 * story is a measurement: the catalogue holds nothing and the downstream projection holds
 * a product — permanently, because no correction will ever be sent.
 */
class DualWriteIntegrationTest extends IntegrationTestBase {

    @Test
    void publishingBeforeTheCommitLeavesAGhostDownstream() throws Exception {
        mockMvc.perform(post("/unsafe/publish-then-rollback")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"sku":"GHOST-01","name":"Never existed","price":99.00}
                                """))
                .andExpect(status().is5xxServerError());

        // @Transactional rolled the database back. The message was already on the broker.
        await().atMost(TIMEOUT).until(() -> projections.count() == 1);

        assertThat(products.count()).as("the catalogue correctly has nothing").isZero();
        assertThat(projections.findAll().getFirst().getSku())
                .as("downstream believes in a product that never existed")
                .isEqualTo("GHOST-01");
        assertThat(outbox.count()).as("the unsafe path never touches the outbox").isZero();
    }

    @Test
    void committingBeforeThePublicationLosesTheEventEntirely() throws Exception {
        mockMvc.perform(post("/unsafe/commit-then-crash")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"sku":"LOST-02","name":"Nobody was told","price":120.00}
                                """))
                .andExpect(status().isOk());

        // The other direction, and the worse one: the data is right and silently invisible
        // to everyone downstream. Nothing will ever retry, because nothing recorded that
        // there was anything to retry.
        assertThat(products.count()).isEqualTo(1);
        assertThat(outbox.count()).isZero();
        assertThat(projections.count()).isZero();
    }

    @Test
    void theOutboxPathKeepsBothSidesConsistent() throws Exception {
        var id = createProduct("SAFE-03", "Properly published", "250.00");

        publisher.publishBatch();
        await().atMost(TIMEOUT).until(() -> projections.findById(id).isPresent());

        // Same two systems, same failure modes available — and they agree.
        assertThat(products.count()).isEqualTo(1);
        assertThat(projections.count()).isEqualTo(1);
    }
}
