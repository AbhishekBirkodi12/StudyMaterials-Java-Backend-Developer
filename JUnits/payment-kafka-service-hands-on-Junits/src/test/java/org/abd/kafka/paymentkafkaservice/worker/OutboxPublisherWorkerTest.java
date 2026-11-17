package org.abd.kafka.paymentkafkaservice.worker;


import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.abd.kafka.paymentkafkaservice.dto.OutboxEvent;
import org.abd.kafka.paymentkafkaservice.dto.PaymentEvent;
import org.abd.kafka.paymentkafkaservice.producer.PaymentProducerService;
import org.abd.kafka.paymentkafkaservice.repository.OutboxRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("OutboxPublisherWorker Unit Tests")
class OutboxPublisherWorkerTest {

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private PaymentProducerService producerService;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private OutboxPublisherWorker worker;

    @Captor
    private ArgumentCaptor<OutboxEvent> outboxCaptor;

    @Test
    @DisplayName("Should do nothing when there are no PENDING outbox records")
    void publishPendingOutboxEvents_whenNoPendingEvents_shouldReturnImmediately() {
        when(outboxRepository.findTop10ByStatusOrderByCreatedAtAsc("PENDING"))
                .thenReturn(Collections.emptyList());

        worker.publishPendingOutboxEvents();

        verify(outboxRepository, times(1)).findTop10ByStatusOrderByCreatedAtAsc("PENDING");
        verifyNoInteractions(objectMapper);
        verifyNoInteractions(producerService);
    }

    @Test
    @DisplayName("Should deserialize payload, publish to Kafka, and mark status as SENT")
    void publishPendingOutboxEvents_success_shouldPublishAndTransitionToSent() throws Exception {
        String payloadJson = "{\"paymentId\":\"PAY-123\"}";
        OutboxEvent outboxRecord = new OutboxEvent(
                "OUTBOX-01",
                "PAYMENT",
                "CUST-01",
                "PAYMENT_CREATED",
                payloadJson,
                "PENDING",
                Instant.now()
        );
        PaymentEvent deserializedEvent = new PaymentEvent(
                "PAY-123", "ORD-1", "CUST-01", new BigDecimal("100"), "INR", "PENDING", Instant.now()
        );

        when(outboxRepository.findTop10ByStatusOrderByCreatedAtAsc("PENDING"))
                .thenReturn(List.of(outboxRecord));
        when(objectMapper.readValue(eq(payloadJson), eq(PaymentEvent.class)))
                .thenReturn(deserializedEvent);

        worker.publishPendingOutboxEvents();

        verify(producerService, times(1)).sendPaymentEvent(deserializedEvent);
        verify(outboxRepository, times(1)).save(outboxCaptor.capture());
        assertEquals("SENT", outboxCaptor.getValue().getStatus());
    }

    @Test
    @DisplayName("Should mark record as FAILED when serialization or Kafka dispatch throws exception")
    void publishPendingOutboxEvents_whenFailureOccurs_shouldMarkRecordAsFailed() throws Exception {
        String invalidPayload = "{malformed-json}";
        OutboxEvent outboxRecord = new OutboxEvent(
                "OUTBOX-ERR",
                "PAYMENT",
                "CUST-FAIL",
                "PAYMENT_CREATED",
                invalidPayload,
                "PENDING",
                Instant.now()
        );

        when(outboxRepository.findTop10ByStatusOrderByCreatedAtAsc("PENDING"))
                .thenReturn(List.of(outboxRecord));
        when(objectMapper.readValue(eq(invalidPayload), eq(PaymentEvent.class)))
                .thenThrow(new JsonParseException(null, "Parsing failed"));

        worker.publishPendingOutboxEvents();

        verifyNoInteractions(producerService);
        verify(outboxRepository, times(1)).save(outboxCaptor.capture());
        assertEquals("FAILED", outboxCaptor.getValue().getStatus());
    }
}
