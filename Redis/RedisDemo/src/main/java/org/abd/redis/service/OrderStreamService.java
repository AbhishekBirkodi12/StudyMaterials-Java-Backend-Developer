package org.abd.redis.service;

import org.abd.redis.model.OrderPlacedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class OrderStreamService {

    private static final Logger log = LoggerFactory.getLogger(OrderStreamService.class);

    public static final String STREAM_KEY = "orders:stream";
    public static final String CONSUMER_GROUP = "orders-group";

    private final RedisTemplate<String, Object> redisTemplate;

    public OrderStreamService(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
        initConsumerGroup();
    }

    private void initConsumerGroup() {
        try {
            redisTemplate.opsForStream().createGroup(STREAM_KEY, ReadOffset.from("0-0"), CONSUMER_GROUP);
            log.info("Consumer group '{}' created on stream '{}'", CONSUMER_GROUP, STREAM_KEY);
        } catch (Exception e) {
            log.info("Consumer group '{}' already active", CONSUMER_GROUP);
        }
    }

    /**
     * Appends an event to the stream (XADD).
     */
    public RecordId publishOrderEvent(OrderPlacedEvent event) {
        Map<String, Object> fields = new HashMap<>();
        fields.put("orderId", event.getOrderId());
        fields.put("customerEmail", event.getCustomerEmail());
        fields.put("totalAmount", String.valueOf(event.getTotalAmount()));

        // Matching MapRecord<String, String, Object> generics cleanly
        MapRecord<String, String, Object> record = MapRecord.create(STREAM_KEY, fields);
        RecordId recordId = redisTemplate.opsForStream().add(record);

        log.info("Published order event to stream: [ID: {}, OrderId: {}]", recordId, event.getOrderId());
        return recordId;
    }

    /**
     * Polls the next undelivered message for a specific worker (XREADGROUP) and ACKs it.
     */
    public boolean pollAndProcessNext(String consumerName) {
        Consumer consumer = Consumer.from(CONSUMER_GROUP, consumerName);
        StreamReadOptions readOptions = StreamReadOptions.empty()
                .count(1)
                .block(Duration.ofMillis(2000));

        // Use wildcard MapRecord<String, ?, ?> to cleanly accept any field/value map structure
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().read(
                consumer,
                readOptions,
                StreamOffset.create(STREAM_KEY, ReadOffset.lastConsumed())
        );

        if (records == null || records.isEmpty()) {
            log.info("[{}] No new messages available in stream.", consumerName);
            return false;
        }

        MapRecord<String, Object, Object> record = records.get(0);
        Map<Object, Object> value = record.getValue();
        RecordId messageId = record.getId();

        log.info("[{}] RECEIVED message [ID: {}] -> OrderId: {}, Email: {}, Amount: ${}",
                consumerName, messageId, value.get("orderId"), value.get("customerEmail"), value.get("totalAmount"));

        try { Thread.sleep(200); } catch (InterruptedException ignored) {}

        Long ackCount = redisTemplate.opsForStream().acknowledge(STREAM_KEY, CONSUMER_GROUP, messageId);
        log.info("[{}] ACKNOWLEDGED message [ID: {}]. (ACK Count: {})", consumerName, messageId, ackCount);

        return true;
    }
}