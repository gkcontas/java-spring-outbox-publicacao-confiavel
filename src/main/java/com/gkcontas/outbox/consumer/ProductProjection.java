package com.gkcontas.outbox.consumer;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The downstream view, in the role a search index or a cache would play.
 *
 * <p>It is written only by the consumer. Nothing in the catalogue touches it, so whatever
 * fails to be published is visibly absent here — which is what makes the dual-write demo
 * legible instead of theoretical.
 */
@Entity
@Table(name = "product_projection")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ProductProjection {

    @Id
    @Column(name = "product_id")
    private UUID productId;

    @Column(nullable = false, length = 40)
    private String sku;

    @Column(nullable = false, length = 160)
    private String name;

    @Column(nullable = false, precision = 12, scale = 2)
    private BigDecimal price;

    @Column(name = "last_event_id", nullable = false)
    private UUID lastEventId;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
