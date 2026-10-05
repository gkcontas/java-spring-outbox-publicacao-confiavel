package com.gkcontas.outbox.api;

import com.gkcontas.outbox.consumer.ProductProjection;
import com.gkcontas.outbox.consumer.ProductProjectionRepository;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The downstream view. Comparing it with {@code /products} is the quickest way to see
 * whether anything was lost on the way out.
 */
@RestController
@RequestMapping("/projection")
class ProjectionController {

    private final ProductProjectionRepository projections;

    ProjectionController(ProductProjectionRepository projections) {
        this.projections = projections;
    }

    record ProjectionView(UUID productId, String sku, String name, BigDecimal price) {
    }

    @GetMapping
    List<ProjectionView> all() {
        return projections.findAll().stream()
                .map(p -> new ProjectionView(p.getProductId(), p.getSku(), p.getName(), p.getPrice()))
                .toList();
    }
}
