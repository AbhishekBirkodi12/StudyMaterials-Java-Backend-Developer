package org.abd;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("Starting Kafka Producer with Keys...");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.22.170.97:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "demo_java";

        // We run two iterations to prove identical keys land on identical partitions
        for (int j = 0; j < 2; j++) {
            log.info("--- Starting Batch {} ---", j + 1);

            for (int i = 0; i < 10; i++) {
                String key = "id_" + i;
                String value = "hello world " + i;

                // 3-argument constructor: Topic, Key, Value
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e == null) {
                            log.info("Key: {} | Partition: {} | Offset: {}",
                                    key,
                                    metadata.partition(),
                                    metadata.offset());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            // Pause between batches to flush socket pipeline
            Thread.sleep(500);
        }

        producer.flush();
        producer.close();
    }
}