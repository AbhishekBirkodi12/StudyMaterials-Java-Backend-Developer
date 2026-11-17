package org.abd.kafka.paymentkafkaservice.repository;

import org.abd.kafka.paymentkafkaservice.dto.OutboxEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
@DisplayName("OutboxRepository DataJpa Slice Tests")
class OutboxRepositoryTest {

    @Autowired
    private OutboxRepository outboxRepository;

    @Autowired
    private TestEntityManager entityManager;

    @Test
    @DisplayName("Should retrieve only PENDING events ordered by createdAt ASC up to a limit of 10")
    void findTop10ByStatusOrderByCreatedAtAsc_shouldReturnOldestPendingEvents() {
        Instant baseTime = Instant.parse("2026-11-01T10:00:00Z");

        // Insert 12 PENDING events
        for (int i = 1; i <= 12; i++) {
            OutboxEvent event = new OutboxEvent(
                    "OUTBOX-" + i,
                    "PAYMENT",
                    "CUST-" + i,
                    "PAYMENT_CREATED",
                    "{}",
                    "PENDING",
                    baseTime.plusSeconds(i * 60)
            );
            entityManager.persist(event);
        }

        // Insert non-PENDING events that should be filtered out
        OutboxEvent sentEvent = new OutboxEvent(
                "OUTBOX-SENT", "PAYMENT", "CUST-SENT", "PAYMENT_CREATED", "{}", "SENT", baseTime
        );
        OutboxEvent failedEvent = new OutboxEvent(
                "OUTBOX-FAILED", "PAYMENT", "CUST-FAIL", "PAYMENT_CREATED", "{}", "FAILED", baseTime
        );
        entityManager.persist(sentEvent);
        entityManager.persist(failedEvent);

        entityManager.flush();
        entityManager.clear();

        List<OutboxEvent> results = outboxRepository.findTop10ByStatusOrderByCreatedAtAsc("PENDING");

        assertEquals(10, results.size(), "Top10 constraint must restrict the query results to 10 entries");
        assertEquals("OUTBOX-1", results.get(0).getId(), "First item should be the oldest PENDING event");
        assertEquals("OUTBOX-10", results.get(9).getId(), "Tenth item should be OUTBOX-10");

        // Verify all fetched events are strictly PENDING
        assertTrue(results.stream().allMatch(e -> "PENDING".equals(e.getStatus())));

        // Verify strictly chronological order (ASC)
        for (int i = 0; i < results.size() - 1; i++) {
            assertTrue(results.get(i).getCreatedAt().isBefore(results.get(i + 1).getCreatedAt())
                    || results.get(i).getCreatedAt().equals(results.get(i + 1).getCreatedAt()));
        }
    }
}
