package org.abd.kafka.paymentkafkaservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.abd.kafka.paymentkafkaservice.dto.PaymentEvent;
import org.abd.kafka.paymentkafkaservice.dto.OutboxEvent;
import org.abd.kafka.paymentkafkaservice.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Service
public class PaymentOrderService {

    private static final Logger log = LoggerFactory.getLogger(PaymentOrderService.class);

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public PaymentOrderService(OutboxRepository outboxRepository, ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public String createPaymentOrder(String customerId, BigDecimal amount) {
        String paymentId = UUID.randomUUID().toString();
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8);

        PaymentEvent event = new PaymentEvent(
                paymentId,
                orderId,
                customerId,
                amount,
                "INR",
                "PENDING",
                Instant.now()
        );

        try {
            String payloadJson = objectMapper.writeValueAsString(event);

            // Outbox entry saved within the same DB transaction
            OutboxEvent outboxEvent = new OutboxEvent(
                    UUID.randomUUID().toString(),
                    "PAYMENT",
                    customerId,
                    "PAYMENT_CREATED",
                    payloadJson,
                    "PENDING",
                    Instant.now()
            );

            outboxRepository.save(outboxEvent);

            log.info("[OUTBOX STORED] Payment '{}' written to outbox table with status PENDING.", paymentId);
            return paymentId;

        } catch (Exception e) {
            log.error("Failed to serialize outbox event: {}", e.getMessage());
            throw new RuntimeException("Transaction rolled back", e);
        }
    }
}