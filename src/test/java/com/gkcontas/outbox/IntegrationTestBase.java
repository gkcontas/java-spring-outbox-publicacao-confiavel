package com.gkcontas.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gkcontas.outbox.consumer.ProcessedEventRepository;
import com.gkcontas.outbox.consumer.ProductProjectionRepository;
import com.gkcontas.outbox.catalog.ProductRepository;
import com.gkcontas.outbox.outbox.OutboxPublisher;
import com.gkcontas.outbox.outbox.OutboxRepository;
import com.gkcontas.outbox.outbox.OutboxStatus;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Both real systems, started once per JVM.
 *
 * <p>There is no way to test this pattern with a mocked broker: the whole subject is what
 * happens when the database commits and the broker does not, which only a real broker can
 * be made to do.
 *
 * <p>The containers are started in a static initialiser rather than through the
 * {@code @Testcontainers} extension — that extension stops a static container when its
 * test class finishes, while Spring caches the context for the next class, which then
 * holds connections to something that no longer exists.
 *
 * <p>The scheduled publisher is switched off: every test calls {@code publishBatch()} at
 * the exact moment it wants, instead of waiting for a timer and hoping.
 */
@SpringBootTest
@AutoConfigureMockMvc
public abstract class IntegrationTestBase {

    protected static final Duration TIMEOUT = Duration.ofSeconds(60);

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("catalog")
            .withUsername("catalog")
            .withPassword("catalog");

    static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    static {
        POSTGRES.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("app.outbox.publisher-enabled", () -> false);

        // Timings tightened for the suite, not for production. The defaults are sized so
        // that a real outage does not hammer the broker; here the outage is deliberate and
        // the test would otherwise spend most of its life waiting for backoff.
        registry.add("app.outbox.initial-backoff", () -> "PT0.2S");
        registry.add("spring.kafka.producer.properties.max.block.ms", () -> 1500);
        registry.add("spring.kafka.producer.properties.request.timeout.ms", () -> 1500);
        registry.add("spring.kafka.producer.properties.delivery.timeout.ms", () -> 2000);
    }

    @Autowired
    protected MockMvc mockMvc;

    @Autowired
    protected ObjectMapper objectMapper;

    @Autowired
    protected OutboxPublisher publisher;

    @Autowired
    protected OutboxRepository outbox;

    @Autowired
    protected ProductRepository products;

    @Autowired
    protected ProductProjectionRepository projections;

    @Autowired
    protected ProcessedEventRepository processedEvents;

    @BeforeEach
    void resetState() {
        outbox.deleteAll();
        products.deleteAll();
        // The projection and the dedup table survive on purpose in the duplicate-delivery
        // test, so they are cleared here and repopulated by each test that needs them.
        projections.deleteAll();
        processedEvents.deleteAll();
    }

    protected UUID createProduct(String sku, String name, String price) throws Exception {
        String body = mockMvc.perform(post("/products")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"sku":"%s","name":"%s","price":%s}
                                """.formatted(sku, name, price)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return UUID.fromString(objectMapper.readTree(body).get("id").asText());
    }

    protected JsonNode json(String path, Object... args) {
        try {
            return objectMapper.readTree(mockMvc
                    .perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                            .get(path, args))
                    .andReturn().getResponse().getContentAsString());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    protected long countByStatus(OutboxStatus status) {
        return outbox.countByStatus(status);
    }

    protected static BigDecimal money(String value) {
        return new BigDecimal(value);
    }
}
