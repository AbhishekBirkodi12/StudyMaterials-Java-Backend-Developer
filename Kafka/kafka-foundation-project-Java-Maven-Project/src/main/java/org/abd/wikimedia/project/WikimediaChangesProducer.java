package org.abd.wikimedia.project;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.Headers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    private static final Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("Starting Wikimedia Changes Kafka Producer...");

        String bootstrapServers = "172.22.170.97:9092";
        String topic = "wikimedia.recentchange";
        String streamUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

        // 1. Basic Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. Safe Producer Configurations (Default in Kafka 3.x, explicit for interview mastery)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        // 3. High Throughput & Compression Tuning
        // Snappy gives an optimal balance between CPU utilization and compression ratio
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // Introduce small 20ms delay to allow micro-batches to fill up
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        // Increase batch size from default 16KB to 32KB
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        // 4. Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 5. Connect SSE EventSource with custom User-Agent Header (fixes HTTP 403 Forbidden)
        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);

        Headers headers = new Headers.Builder()
                .add("User-Agent", "KafkaWikimediaProducerApp/1.0 (abhishek@test.com)")
                .build();

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(streamUrl))
                .headers(headers);

        EventSource eventSource = builder.build();

        // Start reading SSE stream in a background worker thread
        eventSource.start();

        // Keep the process alive for 10 minutes to stream data
        TimeUnit.MINUTES.sleep(10);
    }
}