package com.gkcontas.outbox.catalog;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * The payloads that leave the service.
 *
 * <p>They are records of plain values, not the JPA entity. Serialising the entity would
 * put its identity, its lazy associations and its column names into a contract that
 * downstream consumers depend on — and from then on a rename in the database would be a
 * breaking change for other teams.
 */
public final class ProductEvents {

    private ProductEvents() {
    }

    public record ProductCreated(UUID productId, String sku, String name, BigDecimal price) {
    }

    /**
     * Carries the previous price as well as the new one.
     *
     * <p>A consumer that only receives the new value cannot tell a raise from a cut
     * without keeping its own history — and asking the catalogue for the old value turns a
     * one-way event into a conversation.
     */
    public record PriceChanged(UUID productId, String sku, BigDecimal oldPrice, BigDecimal newPrice) {
    }
}
