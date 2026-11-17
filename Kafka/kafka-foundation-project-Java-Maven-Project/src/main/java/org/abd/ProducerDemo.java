package org.abd;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Starting Kafka Producer...");

        // 1. Create Producer Properties
        Properties properties = new Properties();

        // Connect to your local KRaft broker running on WSL 2 / localhost
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.22.170.97:9092");

        // Serializers: Convert Java String objects into raw bytes for network transport
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. Instantiate KafkaProducer (Thread-safe singleton in production)
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 3. Create a ProducerRecord (Topic + Value)
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "hello world from java");

        // 4. Send Data - Asynchronous operation!
        producer.send(producerRecord);

        // 5. Flush & Close - Ensure memory buffer is sent over the wire before main exits
        producer.flush(); // Blocks until all buffered records are delivered
        producer.close(); // Flushes and cleanly shuts down internal I/O threads

        log.info("Message sent successfully and producer closed.");
    }
}