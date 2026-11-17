package org.abd.kafka.paymentkafkaservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.abd.kafka.paymentkafkaservice.dto.OutboxEvent;
import org.abd.kafka.paymentkafkaservice.dto.PaymentEvent;
import org.abd.kafka.paymentkafkaservice.repository.OutboxRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("PaymentOrderService Unit Tests")
class PaymentOrderServiceTest {

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private PaymentOrderService paymentOrderService;

    @Captor
    private ArgumentCaptor<OutboxEvent> outboxEventCaptor;

    @Captor
    private ArgumentCaptor<PaymentEvent> paymentEventCaptor;

    @Nested
    @DisplayName("createPaymentOrder Tests")
    class CreatePaymentOrderScenarios {

        @Test
        @DisplayName("Should successfully construct, serialize, and store OutboxEvent with correct payload")
        void createPaymentOrder_success_shouldSaveOutboxEventAndReturnPaymentId() throws Exception {
            // Arrange
            String customerId = "CUST-98765";
            BigDecimal amount = new BigDecimal("1499.50");
            String simulatedJson = "{\"paymentId\":\"simulated-id\",\"amount\":1499.50}";

            // Stub object serialization
            when(objectMapper.writeValueAsString(any(PaymentEvent.class))).thenReturn(simulatedJson);

            // Act
            String paymentId = paymentOrderService.createPaymentOrder(customerId, amount);

            // Assert - Step 1: Return value checks
            assertNotNull(paymentId, "Payment ID should not be null");
            assertDoesNotThrow(() -> java.util.UUID.fromString(paymentId), "Payment ID must be a valid UUID string");

            // Assert - Step 2: Verify ObjectMapper interaction and inspect internal PaymentEvent payload
            verify(objectMapper, times(1)).writeValueAsString(paymentEventCaptor.capture());
            PaymentEvent capturedPaymentEvent = paymentEventCaptor.getValue();

            assertAll("PaymentEvent payload validation",
                    () -> assertEquals(paymentId, capturedPaymentEvent.getPaymentId()),
                    () -> assertNotNull(capturedPaymentEvent.getOrderId()),
                    () -> assertTrue(capturedPaymentEvent.getOrderId().startsWith("ORD-")),
                    () -> assertEquals(customerId, capturedPaymentEvent.getCustomerId()),
                    () -> assertEquals(amount, capturedPaymentEvent.getAmount()),
                    () -> assertEquals("INR", capturedPaymentEvent.getCurrency()),
                    () -> assertEquals("PENDING", capturedPaymentEvent.getStatus()),
                    () -> assertNotNull(capturedPaymentEvent.getTimestamp())
            );

            // Assert - Step 3: Verify OutboxRepository interaction and inspect OutboxEvent entity
            verify(outboxRepository, times(1)).save(outboxEventCaptor.capture());
            OutboxEvent capturedOutbox = outboxEventCaptor.getValue();

            assertAll("OutboxEvent entity fields validation",
                    () -> assertNotNull(capturedOutbox.getId()),
                    () -> assertEquals("PAYMENT", capturedOutbox.getAggregateType()),
                    () -> assertEquals(customerId, capturedOutbox.getAggregateId()),
                    () -> assertEquals("PAYMENT_CREATED", capturedOutbox.getEventType()),
                    () -> assertEquals(simulatedJson, capturedOutbox.getPayload()),
                    () -> assertEquals("PENDING", capturedOutbox.getStatus()),
                    () -> assertNotNull(capturedOutbox.getCreatedAt())
            );
        }

        @Test
        @DisplayName("Should throw RuntimeException and never save to outbox when JSON serialization fails")
        void createPaymentOrder_whenSerializationFails_shouldThrowRuntimeExceptionAndSkipSave() throws Exception {
            // Arrange
            String customerId = "CUST-FAIL";
            BigDecimal amount = new BigDecimal("250.00");

            when(objectMapper.writeValueAsString(any(PaymentEvent.class)))
                    .thenThrow(new JsonProcessingException("Simulated JSON serialization failure") {
                    });

            // Act & Assert
            RuntimeException exception = assertThrows(RuntimeException.class, () ->
                    paymentOrderService.createPaymentOrder(customerId, amount)
            );

            assertEquals("Transaction rolled back", exception.getMessage());
            assertInstanceOf(JsonProcessingException.class, exception.getCause());

            // Guarantee DB write was bypassed
            verify(outboxRepository, never()).save(any());
        }

        @Test
        @DisplayName("Should wrap and throw RuntimeException when repository save fails")
        void createPaymentOrder_whenRepositoryFails_shouldPropagateException() throws Exception {
            // Arrange
            String customerId = "CUST-DB-ERROR";
            BigDecimal amount = new BigDecimal("500.00");

            when(objectMapper.writeValueAsString(any(PaymentEvent.class))).thenReturn("{\"mock\":\"json\"}");
            when(outboxRepository.save(any(OutboxEvent.class)))
                    .thenThrow(new DataAccessException("Database connection timeout") {
                    });

            // Act & Assert
            RuntimeException exception = assertThrows(RuntimeException.class, () ->
                    paymentOrderService.createPaymentOrder(customerId, amount)
            );

            assertEquals("Transaction rolled back", exception.getMessage());
            assertInstanceOf(DataAccessException.class, exception.getCause());

            verify(outboxRepository, times(1)).save(any(OutboxEvent.class));
        }
    }
}
