package com.gkcontas.outbox.catalog;

import com.gkcontas.outbox.config.OutboxProperties;
import java.math.BigDecimal;
import java.time.Clock;
import java.util.UUID;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The dual write, written out so it can be run and measured.
 *
 * <p>Both methods here are what people write before they meet the problem, and both are
 * wrong in opposite directions. Keeping them in the codebase, behind an explicit
 * {@code /unsafe} path, is what turns "the outbox prevents X" from a claim into a test.
 *
 * <p>Nothing in this class should be copied into a real service. It is a specimen.
 */
@Service
public class UnsafeProductService {

    private final ProductRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxProperties properties;
    private final Clock clock;

    public UnsafeProductService(ProductRepository repository,
                                KafkaTemplate<String, String> kafkaTemplate,
                                OutboxProperties properties,
                                Clock clock) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * Publishes inside the transaction, then rolls back.
     *
     * <p>{@code @Transactional} covers the database and nothing else: the message is
     * already on the broker when the rollback happens. Downstream now holds a product that
     * does not exist, and no amount of retrying will ever correct it, because nothing will
     * ever be sent to say so.
     */
    @Transactional
    public UUID publishThenRollback(String sku, String name, BigDecimal price) {
        var now = clock.instant();
        Product product = repository.save(
                new Product(UUID.randomUUID(), sku, name, price, now, now));

        kafkaTemplate.send(properties.topic(), product.getId().toString(),
                """
                {"productId":"%s","sku":"%s","name":"%s","price":%s}
                """.formatted(product.getId(), sku, name, price));

        throw new SimulatedFailureException("Simulated failure after the message was sent");
    }

    /**
     * Commits, then publishes — and the process dies in between.
     *
     * <p>The other half of the problem. This one is harder to demonstrate on demand, so
     * the gap is simulated by simply not sending. The point is that the window exists in
     * every "commit first, publish after" implementation, and the only thing hiding it is
     * that the process usually survives those few milliseconds.
     */
    @Transactional
    public UUID commitThenCrash(String sku, String name, BigDecimal price) {
        var now = clock.instant();
        Product product = repository.save(
                new Product(UUID.randomUUID(), sku, name, price, now, now));
        // The send that never happens, because the process stops here.
        return product.getId();
    }
}
