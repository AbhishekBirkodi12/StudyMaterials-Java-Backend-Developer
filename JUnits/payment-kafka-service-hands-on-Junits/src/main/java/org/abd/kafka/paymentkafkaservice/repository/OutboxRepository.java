package org.abd.kafka.paymentkafkaservice.repository;

import org.abd.kafka.paymentkafkaservice.dto.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OutboxRepository extends JpaRepository<OutboxEvent, String> {
    List<OutboxEvent> findTop10ByStatusOrderByCreatedAtAsc(String status);
}