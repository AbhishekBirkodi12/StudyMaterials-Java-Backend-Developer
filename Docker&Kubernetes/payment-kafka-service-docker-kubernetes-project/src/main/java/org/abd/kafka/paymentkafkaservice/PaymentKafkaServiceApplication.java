package org.abd.kafka.paymentkafkaservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class PaymentKafkaServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(PaymentKafkaServiceApplication.class, args);
    }
}