package com.gkcontas.outbox.api;

import com.gkcontas.outbox.catalog.UnsafeProductService;
import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The two wrong ways, kept reachable so the difference can be observed rather than
 * explained.
 */
@RestController
@RequestMapping("/unsafe")
class UnsafeController {

    private final UnsafeProductService unsafe;

    UnsafeController(UnsafeProductService unsafe) {
        this.unsafe = unsafe;
    }

    record UnsafeRequest(String sku, String name, BigDecimal price) {
    }

    /** Publishes, then rolls back. The message is already gone. */
    @PostMapping("/publish-then-rollback")
    void publishThenRollback(@RequestBody UnsafeRequest request) {
        unsafe.publishThenRollback(request.sku(), request.name(), request.price());
    }

    /** Commits, and the publication never happens. The data exists and nobody knows. */
    @PostMapping("/commit-then-crash")
    UUID commitThenCrash(@RequestBody UnsafeRequest request) {
        return unsafe.commitThenCrash(request.sku(), request.name(), request.price());
    }
}
