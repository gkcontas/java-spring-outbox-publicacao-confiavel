package com.gkcontas.outbox.outbox;

import jakarta.persistence.LockModeType;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * The publisher's claim query.
     *
     * <p>{@code FOR UPDATE SKIP LOCKED} is what makes more than one instance of the
     * publisher safe. Without the lock, two instances read the same rows and publish every
     * event twice; with a plain {@code FOR UPDATE} and no {@code SKIP LOCKED}, the second
     * instance blocks waiting for the first instead of picking up different work.
     *
     * <p>Hibernate renders the hint as {@code -2}, which PostgreSQL reads as
     * {@code SKIP LOCKED}; the alternative, {@code jakarta.persistence.lock.timeout = 0},
     * means {@code NOWAIT} and would make the second instance fail rather than skip.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@jakarta.persistence.QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query("""
            SELECT e FROM OutboxEvent e
            WHERE e.status = com.gkcontas.outbox.outbox.OutboxStatus.PENDING
              AND e.nextAttemptAt <= :now
            ORDER BY e.createdAt
            """)
    List<OutboxEvent> claimBatch(@Param("now") Instant now, Limit limit);

    List<OutboxEvent> findByStatusOrderByCreatedAtDesc(OutboxStatus status, Limit limit);

    long countByStatus(OutboxStatus status);

    @Modifying
    @Query("""
            DELETE FROM OutboxEvent e
            WHERE e.status = com.gkcontas.outbox.outbox.OutboxStatus.PUBLISHED
              AND e.publishedAt < :before
            """)
    int deletePublishedBefore(@Param("before") Instant before);
}
