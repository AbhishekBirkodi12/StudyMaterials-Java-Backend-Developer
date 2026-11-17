package org.abd.wikimedia.project;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public WikimediaChangeHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        log.info("SSE Stream connection to Wikimedia opened.");
    }

    @Override
    public void onClosed() {
        log.info("SSE Stream connection closed. Shutting down producer...");
        producer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        log.info("Received event data: {}", messageEvent.getData());

        // Asynchronously send to Kafka
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, messageEvent.getData());
        producer.send(record);
    }

    @Override
    public void onComment(String comment) {
        // SSE comments / heartbeats; no action required
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error reading Wikimedia SSE stream", t);
    }
}