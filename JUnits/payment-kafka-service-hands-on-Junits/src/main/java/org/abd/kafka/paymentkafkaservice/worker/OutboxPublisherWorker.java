package org.abd.kafka.paymentkafkaservice.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.abd.kafka.paymentkafkaservice.dto.PaymentEvent;
import org.abd.kafka.paymentkafkaservice.dto.OutboxEvent;
import org.abd.kafka.paymentkafkaservice.producer.PaymentProducerService;
import org.abd.kafka.paymentkafkaservice.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
public class OutboxPublisherWorker {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherWorker.class);

    private final OutboxRepository outboxRepository;
    private final PaymentProducerService producerService;
    private final ObjectMapper objectMapper;

    public OutboxPublisherWorker(OutboxRepository outboxRepository,
                                 PaymentProducerService producerService,
                                 ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.producerService = producerService;
        this.objectMapper = objectMapper;
    }

    // Runs every 3 seconds to pick up pending outbox records
    @Scheduled(fixedDelay = 3000)
    @Transactional
    public void publishPendingOutboxEvents() {
        List<OutboxEvent> pendingEvents = outboxRepository.findTop10ByStatusOrderByCreatedAtAsc("PENDING");

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info("[OUTBOX RELAY] Found {} pending events to publish...", pendingEvents.size());

        for (OutboxEvent record : pendingEvents) {
            try {
                PaymentEvent event = objectMapper.readValue(record.getPayload(), PaymentEvent.class);

                // Publish to Kafka
                producerService.sendPaymentEvent(event);

                // Mark status as SENT
                record.setStatus("SENT");
                outboxRepository.save(record);

                log.info("[OUTBOX RELAY] Outbox entry '{}' transitioned to SENT.", record.getId());

            } catch (Exception ex) {
                log.error("[OUTBOX RELAY] Failed to publish outbox record '{}': {}", record.getId(), ex.getMessage());
                record.setStatus("FAILED");
                outboxRepository.save(record);
            }
        }
    }
}