package org.abd.kafka.paymentkafkaservice.repository;

import org.abd.kafka.paymentkafkaservice.dto.ProcessedEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DataJpaTest
@DisplayName("ProcessedEventRepository DataJpa Slice Tests")
class ProcessedEventRepositoryTest {

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private TestEntityManager entityManager;

    @Test
    @DisplayName("existsByEventId should return true when event exists and false otherwise")
    void existsByEventId_scenarios() {
        String existingEventId = "EVENT-UUID-001";
        ProcessedEvent processedEvent = new ProcessedEvent(
                existingEventId,
                "PAYMENT_PROCESSED",
                Instant.now()
        );

        entityManager.persistAndFlush(processedEvent);
        entityManager.clear();

        assertTrue(processedEventRepository.existsByEventId(existingEventId));
        assertFalse(processedEventRepository.existsByEventId("NON-EXISTENT-ID"));
    }
}
