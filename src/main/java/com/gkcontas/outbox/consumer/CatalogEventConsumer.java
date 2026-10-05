package com.gkcontas.outbox.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.Clock;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The downstream side, and the half of the pattern people skip.
 *
 * <p>An outbox makes the event impossible to lose; it does nothing to make it arrive once.
 * The deduplication table and the projection update are written in the same transaction,
 * so either the event is recorded as processed and the projection reflects it, or neither
 * happened and the message will be redelivered.
 *
 * <p>Checking "have I seen this id" and then writing is only safe because of that shared
 * transaction. Two instances racing on the same event both pass the check, and the second
 * one fails on the primary key of {@code processed_event} — which is the behaviour wanted:
 * the duplicate is rejected by the database, not by hopeful code.
 */
@Component
public class CatalogEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(CatalogEventConsumer.class);

    private final ProductProjectionRepository projections;
    private final ProcessedEventRepository processed;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public CatalogEventConsumer(ProductProjectionRepository projections,
                                ProcessedEventRepository processed,
                                ObjectMapper objectMapper,
                                Clock clock) {
        this.projections = projections;
        this.processed = processed;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    @KafkaListener(topics = "${app.outbox.topic:catalog-events}", groupId = "catalog-projection")
    @Transactional
    public void on(@Payload String payload,
                   @Header(name = "event-id", required = false) String eventId,
                   @Header(name = "event-type", required = false) String eventType,
                   @Header(KafkaHeaders.RECEIVED_KEY) String key) throws Exception {

        if (eventId == null) {
            // Messages without the header come from the unsafe endpoints, which publish
            // straight to the topic. They are processed so the demonstration is honest
            // about what downstream would really have done with them.
            log.warn("Message without event-id on key {} — treating as a raw publication", key);
            apply(objectMapper.readTree(payload), "ProductCreated", UUID.randomUUID());
            return;
        }

        UUID id = UUID.fromString(eventId);
        if (processed.existsById(id)) {
            log.info("Ignoring duplicate delivery of {}", id);
            return;
        }

        apply(objectMapper.readTree(payload), eventType, id);
        processed.save(new ProcessedEvent(id, clock.instant()));
    }

    private void apply(JsonNode payload, String eventType, UUID eventId) {
        UUID productId = UUID.fromString(payload.get("productId").asText());

        if ("PriceChanged".equals(eventType)) {
            projections.findById(productId).ifPresent(projection -> {
                projection.setPrice(new BigDecimal(payload.get("newPrice").asText()));
                projection.setLastEventId(eventId);
                projection.setUpdatedAt(clock.instant());
            });
            return;
        }

        projections.save(new ProductProjection(
                productId,
                payload.get("sku").asText(),
                payload.get("name").asText(),
                new BigDecimal(payload.get("price").asText()),
                eventId,
                clock.instant()));
    }
}
