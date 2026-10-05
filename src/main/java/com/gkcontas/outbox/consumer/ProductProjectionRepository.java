package com.gkcontas.outbox.consumer;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductProjectionRepository extends JpaRepository<ProductProjection, UUID> {
}
