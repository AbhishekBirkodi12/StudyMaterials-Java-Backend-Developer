package org.abd;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Starting Kafka Consumer with Shutdown Hook...");

        String topic = "demo_java";
        String groupId = "my-java-application";

        // 1. Consumer Configuration Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.22.170.97:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // "earliest" = start from offset 0 if no committed offsets exist (equivalent to --from-beginning)
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 2. Instantiate Consumer (One per thread)
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 3. Register Graceful Shutdown Hook
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal detected! Calling consumer.wakeup()...");
            consumer.wakeup();

            // Wait for main thread to exit the poll loop and run cleanup
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                log.error("Shutdown hook interrupted", e);
            }
        }));

        // 4. Poll Loop inside try-catch-finally
        try {
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Received -> Key: {} | Value: {} | Partition: {} | Offset: {}",
                            record.key(),
                            record.value(),
                            record.partition(),
                            record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer received wakeup signal, beginning shutdown sequence...");
        } catch (Exception e) {
            log.error("Unexpected exception in consumer loop", e);
        } finally {
            log.info("Closing Kafka consumer and committing offsets...");
            consumer.close();
            log.info("The consumer has gracefully stopped.");
        }
    }
}