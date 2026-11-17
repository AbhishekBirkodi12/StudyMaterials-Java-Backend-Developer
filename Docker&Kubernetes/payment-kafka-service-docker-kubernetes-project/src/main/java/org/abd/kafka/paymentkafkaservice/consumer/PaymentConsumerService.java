package org.abd.kafka.paymentkafkaservice.consumer;

import org.abd.kafka.paymentkafkaservice.dto.PaymentEvent;
import org.abd.kafka.paymentkafkaservice.dto.ProcessedEvent;
import org.abd.kafka.paymentkafkaservice.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class PaymentConsumerService {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumerService.class);

    private final ProcessedEventRepository processedEventRepository;

    public PaymentConsumerService(ProcessedEventRepository processedEventRepository) {
        this.processedEventRepository = processedEventRepository;
    }

    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            autoCreateTopics = "true",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltStrategy = DltStrategy.FAIL_ON_ERROR
    )
    @KafkaListener(
            topics = "${app.kafka.topics.payment-events}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void consumePaymentEvent(
            @Payload PaymentEvent event,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment
    ) {
        String eventId = event.getPaymentId();
        log.info("[CONSUMER] Received eventId='{}' from topic='{}', partition={}, offset={}",
                eventId, topic, partition, offset);

        // 1. Idempotency Check (Check-Then-Act)
        if (processedEventRepository.existsByEventId(eventId)) {
            log.warn("[IDEMPOTENCY GUARD] Duplicate event detected for paymentId='{}'. Skipping execution & committing offset.", eventId);
            acknowledgment.acknowledge();
            return;
        }

        // 2. Simulated Outage test branch
        if ("FAIL".equalsIgnoreCase(event.getCustomerId())) {
            log.warn("[CONSUMER] Simulating transient failure for paymentId='{}'!", eventId);
            throw new RuntimeException("Downstream Gateway Timeout for: " + event.getCustomerId());
        }

        // 3. Atomically Record Event ID & Execute Business Logic
        try {
            processedEventRepository.save(new ProcessedEvent(eventId, "PAYMENT_PROCESSED", Instant.now()));
            processTransaction(event);

            // 4. Manual offset commit
            acknowledgment.acknowledge();
            log.info("[CONSUMER] Payment '{}' settled and recorded successfully.", eventId);
        } catch (DataIntegrityViolationException ex) {
            // Concurrent race condition safeguard: duplicate key constraint hit at database level
            log.warn("[CONCURRENCY GUARD] Duplicate key constraint triggered for eventId='{}'. Acknowledging offset.", eventId);
            acknowledgment.acknowledge();
        }
    }

    @DltHandler
    public void handleDlt(
            @Payload PaymentEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        log.error("[DLT] Poison pill routed to DLT. Topic='{}', offset={}, paymentId='{}'",
                topic, offset, event.getPaymentId());
        acknowledgment.acknowledge();
    }

    private void processTransaction(PaymentEvent event) {
        log.info("[BUSINESS LOGIC] Deducting INR {} from customer '{}' for order '{}'",
                event.getAmount(), event.getCustomerId(), event.getOrderId());
    }
}