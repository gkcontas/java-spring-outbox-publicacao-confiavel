package com.gkcontas.outbox;

import com.gkcontas.outbox.outbox.OutboxStatus;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Two publishers at once, which is what a second instance of the service looks like.
 *
 * <p>This is where a naive outbox falls apart. Both instances run the same "give me the
 * pending rows" query at the same second, both get the same rows, and every event is
 * delivered twice — or, with a plain {@code FOR UPDATE}, one instance sits blocked behind
 * the other and the throughput of the pair is the throughput of one.
 *
 * <p>{@code SKIP LOCKED} makes the claim exclusive and non-blocking: whoever gets there
 * first owns the row, and the other moves on to different work.
 */
class ConcurrentPublisherIntegrationTest extends IntegrationTestBase {

    @Test
    void deliversEachEventOnceWhenTwoPublishersRunTogether() throws Exception {
        for (int index = 0; index < 12; index++) {
            createProduct("CONC-%02d".formatted(index), "Concurrent", "10.00");
        }
        assertThat(countByStatus(OutboxStatus.PENDING)).isEqualTo(12);

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            List<Callable<Integer>> publishers = List.of(
                    publisher::publishBatch, publisher::publishBatch);
            int delivered = 0;
            for (Future<Integer> result : executor.invokeAll(publishers)) {
                delivered += result.get();
            }
            // Between them they publish every row, and no row twice.
            assertThat(delivered).isEqualTo(12);
        }

        assertThat(countByStatus(OutboxStatus.PENDING)).isZero();
        assertThat(countByStatus(OutboxStatus.PUBLISHED)).isEqualTo(12);

        await().atMost(TIMEOUT).until(() -> projections.count() == 12);
        // The consumer would have deduplicated a repeat, so this is the stronger check:
        // it never saw one.
        assertThat(processedEvents.count()).isEqualTo(12);
    }
}
