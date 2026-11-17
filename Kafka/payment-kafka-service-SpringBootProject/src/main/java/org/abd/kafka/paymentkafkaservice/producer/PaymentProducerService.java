package org.abd.kafka.paymentkafkaservice.producer;


import org.abd.kafka.paymentkafkaservice.dto.PaymentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class PaymentProducerService {

    private static final Logger log = LoggerFactory.getLogger(PaymentProducerService.class);

    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    @Value("${app.kafka.topics.payment-events}")
    private String topicName;

    public PaymentProducerService(KafkaTemplate<String, PaymentEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, PaymentEvent>> sendPaymentEvent(PaymentEvent event) {
        String key = event.getCustomerId();

        log.info("Dispatching PaymentEvent to topic='{}', key='{}', payload={}", topicName, key, event);

        CompletableFuture<SendResult<String, PaymentEvent>> future = kafkaTemplate.send(topicName, key, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("PaymentEvent sent successfully! Key='{}', Partition={}, Offset={}, Timestamp={}",
                        key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        result.getRecordMetadata().timestamp());
            } else {
                log.error("Failed to send PaymentEvent for key='{}' due to: {}", key, ex.getMessage(), ex);
            }
        });

        return future;
    }
}