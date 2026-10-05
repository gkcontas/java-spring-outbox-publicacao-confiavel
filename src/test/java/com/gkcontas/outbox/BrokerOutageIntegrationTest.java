package com.gkcontas.outbox;

import com.gkcontas.outbox.outbox.OutboxStatus;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The broker goes away, and nothing is lost.
 *
 * <p>This is the claim that justifies the pattern, so it is tested against a broker that
 * is genuinely unreachable rather than a mock that throws. Pausing the container freezes
 * its processes while the port stays bound, which is what a real network partition looks
 * like to a producer: connections that hang rather than refuse.
 */
class BrokerOutageIntegrationTest extends IntegrationTestBase {

    private boolean paused;

    private void pauseBroker() {
        DockerClientFactory.instance().client().pauseContainerCmd(KAFKA.getContainerId()).exec();
        paused = true;
    }

    private void resumeBroker() {
        if (paused) {
            DockerClientFactory.instance().client().unpauseContainerCmd(KAFKA.getContainerId()).exec();
            paused = false;
        }
    }

    @AfterEach
    void alwaysResume() {
        // A test that leaves the broker frozen takes every later test down with it.
        resumeBroker();
    }

    @Test
    void keepsEventsPendingWhileTheBrokerIsDownAndDeliversThemAfterwards() throws Exception {
        pauseBroker();

        UUID first = createProduct("OFF-01", "Created during the outage", "11.00");
        UUID second = createProduct("OFF-02", "Also during the outage", "12.00");

        // The business operation is unaffected: it never touched the broker.
        assertThat(products.count()).isEqualTo(2);
        assertThat(countByStatus(OutboxStatus.PENDING)).isEqualTo(2);

        // Publishing fails, and says so in the row rather than throwing the batch away.
        assertThat(publisher.publishBatch()).isZero();
        assertThat(outbox.findAll()).allSatisfy(event -> {
            assertThat(event.getAttempts()).isPositive();
            assertThat(event.getLastError()).isNotBlank();
            assertThat(event.getStatus()).isEqualTo(OutboxStatus.PENDING);
        });

        resumeBroker();

        // Driving the publisher and waiting on the queue, not on a return value: the
        // backoff can make the two rows become due at different moments, so a batch may
        // well deliver one now and the other on the next pass.
        await().atMost(TIMEOUT).until(() -> {
            publisher.publishBatch();
            return countByStatus(OutboxStatus.PENDING) == 0;
        });

        await().atMost(TIMEOUT).until(() ->
                projections.findById(first).isPresent() && projections.findById(second).isPresent());
        assertThat(countByStatus(OutboxStatus.PUBLISHED)).isEqualTo(2);
    }

    @Test
    void parksAnEventThatExhaustsItsAttempts() throws Exception {
        pauseBroker();
        createProduct("DEAD-03", "Never gets through", "13.00");

        // Five attempts is the configured ceiling. A row nobody can publish has to stop
        // being retried, or it blocks the batch it sits in forever.
        await().atMost(TIMEOUT).until(() -> {
            publisher.publishBatch();
            return countByStatus(OutboxStatus.FAILED) == 1;
        });

        resumeBroker();
        assertThat(publisher.publishBatch()).as("a parked event is not picked up again").isZero();

        // Parked, not discarded: the payload is intact and an operator can requeue it.
        assertThat(publisher.requeueFailed()).isEqualTo(1);
        await().atMost(TIMEOUT).until(() -> publisher.publishBatch() == 1);
        assertThat(countByStatus(OutboxStatus.PUBLISHED)).isEqualTo(1);
    }
}
