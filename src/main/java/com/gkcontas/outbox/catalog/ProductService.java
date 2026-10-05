package com.gkcontas.outbox.catalog;

import com.gkcontas.outbox.outbox.OutboxRecorder;
import java.math.BigDecimal;
import java.time.Clock;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Where the pattern actually lives: one transaction, two writes to the same database.
 *
 * <p>The product row and the outbox row are committed together. There is no broker in this
 * class and no network call — which is exactly why the atomicity holds. Everything that
 * follows is the publisher's problem, and the publisher can retry forever without the
 * caller knowing.
 */
@Service
public class ProductService {

    static final String AGGREGATE_TYPE = "product";

    private final ProductRepository repository;
    private final OutboxRecorder outbox;
    private final Clock clock;

    public ProductService(ProductRepository repository, OutboxRecorder outbox, Clock clock) {
        this.repository = repository;
        this.outbox = outbox;
        this.clock = clock;
    }

    @Transactional
    public Product create(String sku, String name, BigDecimal price) {
        if (repository.existsBySku(sku)) {
            throw new DuplicateSkuException(sku);
        }
        var now = clock.instant();
        Product product = repository.save(
                new Product(UUID.randomUUID(), sku, name, price, now, now));

        outbox.record(AGGREGATE_TYPE, product.getId().toString(),
                new ProductEvents.ProductCreated(product.getId(), product.getSku(),
                        product.getName(), product.getPrice()));

        return product;
    }

    @Transactional
    public Product changePrice(UUID productId, BigDecimal newPrice) {
        Product product = repository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException(productId));

        BigDecimal oldPrice = product.getPrice();
        if (oldPrice.compareTo(newPrice) == 0) {
            // No change, no event. Publishing one anyway would make every consumer
            // reprocess for nothing, and would make "price changed" stop meaning it.
            return product;
        }

        product.setPrice(newPrice);
        product.setUpdatedAt(clock.instant());

        outbox.record(AGGREGATE_TYPE, product.getId().toString(),
                new ProductEvents.PriceChanged(product.getId(), product.getSku(), oldPrice, newPrice));

        return product;
    }

    /**
     * A price change that fails after the event was recorded.
     *
     * <p>It exists to show the half of the guarantee that is easy to forget: the outbox row
     * is rolled back with the business change, so a failed operation emits nothing. A
     * design that published before the commit would already have told the world.
     */
    @Transactional
    public void changePriceThenFail(UUID productId, BigDecimal newPrice) {
        changePrice(productId, newPrice);
        throw new SimulatedFailureException("Simulated failure after recording the event");
    }

    @Transactional(readOnly = true)
    public List<Product> findAll() {
        return repository.findAll();
    }

    @Transactional(readOnly = true)
    public Product byId(UUID id) {
        return repository.findById(id).orElseThrow(() -> new ProductNotFoundException(id));
    }
}
