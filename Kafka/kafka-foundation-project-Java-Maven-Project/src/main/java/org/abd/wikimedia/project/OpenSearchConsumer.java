package org.abd.wikimedia.project;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    // 1. Create OpenSearch Client
    public static RestHighLevelClient createOpenSearchClient() {
        // Use WSL 2 bridge IP: 172.22.170.97
        return new RestHighLevelClient(
                RestClient.builder(new HttpHost("172.22.170.97", 9200, "http"))
        );
    }

    // 2. Create Kafka Consumer with Manual Commit Configuration
    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String bootstrapServers = "172.22.170.97:9092";
        String groupId = "consumer-opensearch-demo";

        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // Disable Auto-Commit for manual at-least-once control
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(properties);
    }

    // 3. Extract unique meta.id from Wikimedia JSON payload using GSON
    private static String extractId(String jsonPayload) {
        return JsonParser.parseString(jsonPayload)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {
        log.info("Starting OpenSearch Kafka Consumer...");

        RestHighLevelClient openSearchClient = createOpenSearchClient();
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // Graceful Shutdown Hook setup
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected shutdown signal. Invoking consumer.wakeup()...");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        try (openSearchClient; consumer) {
            // Idempotently create 'wikimedia' index if it does not already exist
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The 'wikimedia' index has been created in OpenSearch.");
            } else {
                log.info("The 'wikimedia' index already exists.");
            }

            // Subscribe consumer to topic
            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            // Polling and Bulk Indexing Loop
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("Received {} records from Kafka", recordCount);

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // Strategy 2: Extract native meta.id from payload for consumer idempotency
                        String id = extractId(record.value());

                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);

                        bulkRequest.add(indexRequest);
                    } catch (Exception e) {
                        log.warn("Skipping bad record or payload without valid meta.id", e);
                    }
                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Successfully indexed {} documents into OpenSearch.", bulkResponse.getItems().length);

                    // Optional short sleep to observe batching rhythm
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    // Commit offsets synchronously only after OpenSearch confirms the bulk write
                    consumer.commitSync();
                    log.info("Offsets committed synchronously.");
                }
            }

        } catch (WakeupException e) {
            log.info("Consumer received wakeup signal. Exiting poll loop...");
        } catch (Exception e) {
            log.error("Unexpected error occurred in OpenSearch Consumer", e);
        } finally {
            try {
                consumer.commitSync();
            } catch (Exception ignored) {
            }
            log.info("Consumer and OpenSearch client closed gracefully.");
        }
    }
}