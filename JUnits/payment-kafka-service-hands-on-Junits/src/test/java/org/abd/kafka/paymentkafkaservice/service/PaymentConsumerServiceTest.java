package org.abd.kafka.paymentkafkaservice.service;

import org.abd.kafka.paymentkafkaservice.consumer.PaymentConsumerService;
import org.abd.kafka.paymentkafkaservice.dto.PaymentEvent;
import org.abd.kafka.paymentkafkaservice.dto.ProcessedEvent;
import org.abd.kafka.paymentkafkaservice.repository.ProcessedEventRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("PaymentConsumerService Unit Tests")
class PaymentConsumerServiceTest {

    @Mock
    private ProcessedEventRepository processedEventRepository;

    @Mock
    private Acknowledgment acknowledgment;

    @InjectMocks
    private PaymentConsumerService consumerService;

    @Captor
    private ArgumentCaptor<ProcessedEvent> processedEventCaptor;

    private PaymentEvent createSampleEvent(String paymentId, String customerId) {
        return new PaymentEvent(
                paymentId,
                "ORD-9901",
                customerId,
                new BigDecimal("1999.00"),
                "INR",
                "PENDING",
                Instant.now()
        );
    }

    @Nested
    @DisplayName("consumePaymentEvent Scenarios")
    class ConsumePaymentEventScenarios {

        @Test
        @DisplayName("Should process new event, save to processed_events, and commit offset")
        void consumePaymentEvent_newEvent_shouldProcessAndAcknowledge() {
            String paymentId = "PAY-UUID-101";
            PaymentEvent event = createSampleEvent(paymentId, "CUST-01");

            when(processedEventRepository.existsByEventId(paymentId)).thenReturn(false);

            consumerService.consumePaymentEvent(event, "CUST-01", 0, 100L, "payment-events", acknowledgment);

            verify(processedEventRepository, times(1)).save(processedEventCaptor.capture());
            ProcessedEvent savedRecord = processedEventCaptor.getValue();
            assertEquals(paymentId, savedRecord.getEventId());
            assertEquals("PAYMENT_PROCESSED", savedRecord.getEventType());
            assertNotNull(savedRecord.getProcessedAt());

            verify(acknowledgment, times(1)).acknowledge();
        }

        @Test
        @DisplayName("Should skip processing and commit offset when event is duplicate")
        void consumePaymentEvent_duplicateEvent_shouldSkipExecutionAndAcknowledge() {
            String paymentId = "STATIC-PAYMENT-UUID-999";
            PaymentEvent event = createSampleEvent(paymentId, "CUST-DUP");

            when(processedEventRepository.existsByEventId(paymentId)).thenReturn(true);

            consumerService.consumePaymentEvent(event, "CUST-DUP", 0, 105L, "payment-events", acknowledgment);

            verify(processedEventRepository, never()).save(any());
            verify(acknowledgment, times(1)).acknowledge();
        }

        @Test
        @DisplayName("Should throw RuntimeException and NOT acknowledge offset when customer is FAIL")
        void consumePaymentEvent_failureSimulation_shouldThrowExceptionAndNotAcknowledge() {
            String paymentId = "PAY-FAIL-777";
            PaymentEvent event = createSampleEvent(paymentId, "FAIL");

            when(processedEventRepository.existsByEventId(paymentId)).thenReturn(false);

            RuntimeException ex = assertThrows(RuntimeException.class, () ->
                    consumerService.consumePaymentEvent(event, "FAIL", 0, 110L, "payment-events", acknowledgment)
            );

            assertTrue(ex.getMessage().contains("Downstream Gateway Timeout"));
            verify(processedEventRepository, never()).save(any());
            verify(acknowledgment, never()).acknowledge();
        }

        @Test
        @DisplayName("Should handle database unique constraint violation and still commit offset")
        void consumePaymentEvent_concurrencyRaceCondition_shouldCatchDataIntegrityViolationAndAcknowledge() {
            String paymentId = "PAY-RACE-888";
            PaymentEvent event = createSampleEvent(paymentId, "CUST-RACE");

            when(processedEventRepository.existsByEventId(paymentId)).thenReturn(false);
            when(processedEventRepository.save(any(ProcessedEvent.class)))
                    .thenThrow(new DataIntegrityViolationException("Duplicate key violation on event_id"));

            assertDoesNotThrow(() ->
                    consumerService.consumePaymentEvent(event, "CUST-RACE", 0, 115L, "payment-events", acknowledgment)
            );

            verify(acknowledgment, times(1)).acknowledge();
        }
    }

    @Nested
    @DisplayName("Dead Letter Queue (DLT) Handler Scenarios")
    class DltHandlerScenarios {

        @Test
        @DisplayName("Should log poisoned message and acknowledge offset from DLT")
        void handleDlt_shouldAcknowledgePoisonPill() {
            PaymentEvent event = createSampleEvent("POISON-PAY-99", "CUST-BAD");

            consumerService.handleDlt(event, "payment-events-dlt", 200L, acknowledgment);

            verify(acknowledgment, times(1)).acknowledge();
        }
    }
}
