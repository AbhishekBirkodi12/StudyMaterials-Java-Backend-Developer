package org.abd.kafka.paymentkafkaservice.controllers;


import org.abd.kafka.paymentkafkaservice.dto.PaymentEvent;
import org.abd.kafka.paymentkafkaservice.producer.PaymentProducerService;
import org.abd.kafka.paymentkafkaservice.service.PaymentOrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/api/payments")
public class PaymentController {

    private final PaymentProducerService producerService;
    private final PaymentOrderService paymentOrderService;




    public PaymentController(PaymentProducerService producerService, PaymentOrderService paymentOrderService) {
        this.producerService = producerService;
        this.paymentOrderService = paymentOrderService;
    }

    @GetMapping("/publish")
    public ResponseEntity<String> publishPayment(
            @RequestParam String customerId,
            @RequestParam BigDecimal amount) {

        PaymentEvent event = new PaymentEvent(
                UUID.randomUUID().toString(),
                "ORD-" + UUID.randomUUID().toString().substring(0, 8),
                customerId,
                amount,
                "INR",
                "PENDING",
                Instant.now()
        );

        producerService.sendPaymentEvent(event);

        return ResponseEntity.ok("PaymentEvent published for customer: " + customerId);
    }


    @GetMapping("/publish-duplicate")
    public ResponseEntity<String> publishDuplicate(@RequestParam String customerId) {
        String fixedPaymentId = "STATIC-PAYMENT-UUID-999";

        PaymentEvent event = new PaymentEvent(
                fixedPaymentId,
                "ORD-FIXED-100",
                customerId,
                new BigDecimal("5000.00"),
                "INR",
                "PENDING",
                Instant.now()
        );

        // Fire message 1
        producerService.sendPaymentEvent(event);

        // Fire message 2 with the identical ID
        producerService.sendPaymentEvent(event);

        return ResponseEntity.ok("Dispatched two identical payment events with ID: " + fixedPaymentId);
    }

    @GetMapping("/outbox-create")
    public ResponseEntity<String> createViaOutbox(
            @RequestParam String customerId,
            @RequestParam BigDecimal amount) {

        String paymentId = paymentOrderService.createPaymentOrder(customerId, amount);
        return ResponseEntity.ok("Order saved atomically in DB Outbox. PaymentId: " + paymentId);
    }
}