package org.abd.kafka.paymentkafkaservice.controllers;

import org.abd.kafka.paymentkafkaservice.dto.PaymentEvent;
import org.abd.kafka.paymentkafkaservice.producer.PaymentProducerService;
import org.abd.kafka.paymentkafkaservice.service.PaymentOrderService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(PaymentController.class)
@DisplayName("PaymentController WebMvc Slice Tests")
class PaymentControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private PaymentProducerService producerService;

    @MockBean
    private PaymentOrderService paymentOrderService;

    @Captor
    private ArgumentCaptor<PaymentEvent> paymentEventCaptor;

    @Nested
    @DisplayName("GET /api/payments/publish")
    class PublishPaymentEndpointTests {

        @Test
        @DisplayName("Should return 200 OK and invoke producerService with valid PaymentEvent")
        void publishPayment_withValidParams_shouldReturn200AndSendMessage() throws Exception {
            String customerId = "CUST-1001";
            String amount = "2500.00";

            mockMvc.perform(get("/api/payments/publish")
                            .param("customerId", customerId)
                            .param("amount", amount))
                    .andExpect(status().isOk())
                    .andExpect(content().string(containsString("PaymentEvent published for customer: " + customerId)));

            verify(producerService, times(1)).sendPaymentEvent(paymentEventCaptor.capture());
            PaymentEvent sentEvent = paymentEventCaptor.getValue();

            assertAll("PaymentEvent contract assertions",
                    () -> assertNotNull(sentEvent.getPaymentId()),
                    () -> assertNotNull(sentEvent.getOrderId()),
                    () -> assertTrue(sentEvent.getOrderId().startsWith("ORD-")),
                    () -> assertEquals(customerId, sentEvent.getCustomerId()),
                    () -> assertEquals(new BigDecimal(amount), sentEvent.getAmount()),
                    () -> assertEquals("INR", sentEvent.getCurrency()),
                    () -> assertEquals("PENDING", sentEvent.getStatus()),
                    () -> assertNotNull(sentEvent.getTimestamp())
            );
        }

        @Test
        @DisplayName("Should return 400 Bad Request when required amount query param is missing")
        void publishPayment_missingAmountParam_shouldReturn400BadRequest() throws Exception {
            mockMvc.perform(get("/api/payments/publish")
                            .param("customerId", "CUST-1001"))
                    .andExpect(status().isBadRequest());

            verifyNoInteractions(producerService);
        }
    }

    @Nested
    @DisplayName("GET /api/payments/publish-duplicate")
    class PublishDuplicateEndpointTests {

        @Test
        @DisplayName("Should dispatch two identical PaymentEvents with fixed ID and return 200 OK")
        void publishDuplicate_shouldDispatchTwoIdenticalEvents() throws Exception {
            String customerId = "CUST-DUP-99";
            String expectedFixedId = "STATIC-PAYMENT-UUID-999";

            mockMvc.perform(get("/api/payments/publish-duplicate")
                            .param("customerId", customerId))
                    .andExpect(status().isOk())
                    .andExpect(content().string(containsString("Dispatched two identical payment events with ID: " + expectedFixedId)));

            verify(producerService, times(2)).sendPaymentEvent(paymentEventCaptor.capture());

            List<PaymentEvent> capturedEvents = paymentEventCaptor.getAllValues();
            assertEquals(2, capturedEvents.size());
            assertEquals(expectedFixedId, capturedEvents.get(0).getPaymentId());
            assertEquals(expectedFixedId, capturedEvents.get(1).getPaymentId());
            assertEquals(new BigDecimal("5000.00"), capturedEvents.get(0).getAmount());
            assertEquals(customerId, capturedEvents.get(0).getCustomerId());
        }

        @Test
        @DisplayName("Should return 400 Bad Request when customerId is missing")
        void publishDuplicate_missingCustomerId_shouldReturn400() throws Exception {
            mockMvc.perform(get("/api/payments/publish-duplicate"))
                    .andExpect(status().isBadRequest());

            verifyNoInteractions(producerService);
        }
    }

    @Nested
    @DisplayName("GET /api/payments/outbox-create")
    class OutboxCreateEndpointTests {

        @Test
        @DisplayName("Should invoke paymentOrderService and return 200 OK with paymentId")
        void createViaOutbox_shouldReturn200AndIncludePaymentId() throws Exception {
            String customerId = "CUST-OUTBOX-77";
            BigDecimal amount = new BigDecimal("1200.00");
            String simulatedPaymentId = "PAY-UUID-445566";

            when(paymentOrderService.createPaymentOrder(eq(customerId), eq(amount)))
                    .thenReturn(simulatedPaymentId);

            mockMvc.perform(get("/api/payments/outbox-create")
                            .param("customerId", customerId)
                            .param("amount", "1200.00"))
                    .andExpect(status().isOk())
                    .andExpect(content().string(containsString("Order saved atomically in DB Outbox. PaymentId: " + simulatedPaymentId)));

            verify(paymentOrderService, times(1)).createPaymentOrder(customerId, amount);
            verifyNoInteractions(producerService);
        }

        @Test
        @DisplayName("Should return 400 Bad Request when customerId or amount is missing")
        void createViaOutbox_missingParameters_shouldReturn400() throws Exception {
            mockMvc.perform(get("/api/payments/outbox-create")
                            .param("customerId", "CUST-OUTBOX-77"))
                    .andExpect(status().isBadRequest());

            verifyNoInteractions(paymentOrderService);
        }
    }
}