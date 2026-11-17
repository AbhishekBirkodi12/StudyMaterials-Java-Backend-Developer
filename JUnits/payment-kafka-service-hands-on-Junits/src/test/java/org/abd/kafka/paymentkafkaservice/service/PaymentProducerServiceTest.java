package org.abd.kafka.paymentkafkaservice.service;

import org.abd.kafka.paymentkafkaservice.dto.PaymentEvent;
import org.abd.kafka.paymentkafkaservice.producer.PaymentProducerService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("PaymentProducerService Unit Tests")
class PaymentProducerServiceTest {

    private static final String TOPIC_NAME = "payment-events";

    @Mock
    private KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    @InjectMocks
    private PaymentProducerService producerService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(producerService, "topicName", TOPIC_NAME);
    }

    private PaymentEvent createSampleEvent(String customerId) {
        return new PaymentEvent(
                "PAY-PROD-001",
                "ORD-9001",
                customerId,
                new BigDecimal("3500.00"),
                "INR",
                "PENDING",
                Instant.now()
        );
    }

    @Test
    @DisplayName("Should send message to Kafka with customerId as partition key and complete future successfully")
    void sendPaymentEvent_success_shouldReturnCompletedFuture() throws Exception {
        PaymentEvent event = createSampleEvent("CUST-999");
        ProducerRecord<String, PaymentEvent> producerRecord = new ProducerRecord<>(TOPIC_NAME, "CUST-999", event);
        RecordMetadata recordMetadata = new RecordMetadata(
                new TopicPartition(TOPIC_NAME, 0),
                0L,
                0,
                System.currentTimeMillis(),
                0,
                0
        );
        SendResult<String, PaymentEvent> sendResult = new SendResult<>(producerRecord, recordMetadata);
        CompletableFuture<SendResult<String, PaymentEvent>> successfulFuture = CompletableFuture.completedFuture(sendResult);

        when(kafkaTemplate.send(eq(TOPIC_NAME), eq("CUST-999"), eq(event)))
                .thenReturn(successfulFuture);

        CompletableFuture<SendResult<String, PaymentEvent>> resultFuture = producerService.sendPaymentEvent(event);

        assertNotNull(resultFuture);
        assertTrue(resultFuture.isDone());
        assertFalse(resultFuture.isCompletedExceptionally());
        assertEquals(sendResult, resultFuture.get());

        verify(kafkaTemplate, times(1)).send(TOPIC_NAME, "CUST-999", event);
    }

    @Test
    @DisplayName("Should handle Kafka dispatch failure and complete future exceptionally")
    void sendPaymentEvent_failure_shouldReturnExceptionallyCompletedFuture() {
        PaymentEvent event = createSampleEvent("CUST-ERROR");
        RuntimeException kafkaException = new RuntimeException("Kafka Broker unavailable");
        CompletableFuture<SendResult<String, PaymentEvent>> failedFuture = CompletableFuture.failedFuture(kafkaException);

        when(kafkaTemplate.send(eq(TOPIC_NAME), eq("CUST-ERROR"), eq(event)))
                .thenReturn(failedFuture);

        CompletableFuture<SendResult<String, PaymentEvent>> resultFuture = producerService.sendPaymentEvent(event);

        assertNotNull(resultFuture);
        assertTrue(resultFuture.isDone());
        assertTrue(resultFuture.isCompletedExceptionally());

        ExecutionException exception = assertThrows(ExecutionException.class, resultFuture::get);
        assertInstanceOf(RuntimeException.class, exception.getCause());
        assertEquals("Kafka Broker unavailable", exception.getCause().getMessage());

        verify(kafkaTemplate, times(1)).send(TOPIC_NAME, "CUST-ERROR", event);
    }
}
