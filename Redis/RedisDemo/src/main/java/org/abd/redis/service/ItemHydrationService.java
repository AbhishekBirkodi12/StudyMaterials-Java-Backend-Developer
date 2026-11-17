package org.abd.redis.service;

import org.abd.redis.model.ItemDto;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class ItemHydrationService {

    private final RedisTemplate<String, Object> redisTemplate;
    private static final String VIEWS_LEADERBOARD = "items:by_views";

    public ItemHydrationService(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * Seeds test items into Redis Hashes and ranks them in a Sorted Set.
     */
    public void seedTestData() {
        List<ItemDto> items = List.of(
                new ItemDto("101", "Mechanical Keyboard", 120.0, 1500L),
                new ItemDto("102", "Ergonomic Mouse", 60.0, 3200L),
                new ItemDto("103", "UltraWide Monitor", 450.0, 8900L),
                new ItemDto("104", "USB-C Hub", 35.0, 450L)
        );

        for (ItemDto item : items) {
            String hashKey = "item:" + item.getId();
            Map<String, Object> hash = new HashMap<>();
            hash.put("id", item.getId());
            hash.put("name", item.getName());
            hash.put("price", String.valueOf(item.getPrice()));
            hash.put("views", String.valueOf(item.getViews()));

            // Store entity hash
            redisTemplate.opsForHash().putAll(hashKey, hash);
            // Index view count in Sorted Set
            redisTemplate.opsForZSet().add(VIEWS_LEADERBOARD, item.getId(), item.getViews());
        }
    }

    /**
     * Hydrates top N items using a two-step query:
     * 1. Query IDs from ZSet (O(log(N) + M))
     * 2. Pipeline HGETALL calls over a single TCP socket stream
     */
    @SuppressWarnings("unchecked")
    public List<ItemDto> getTopItemsByViews(int limit) {
        // Step 1: Get top IDs sorted by highest views first
        Set<Object> topIds = redisTemplate.opsForZSet()
                .reverseRange(VIEWS_LEADERBOARD, 0, limit - 1);

        if (topIds == null || topIds.isEmpty()) {
            return Collections.emptyList();
        }

        // Step 2: Batch HGETALL operations across a pipeline (1 network round-trip)
        List<Object> rawResults = redisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                for (Object id : topIds) {
                    operations.opsForHash().entries("item:" + id);
                }
                return null; // Mandatory in executePipelined: operations are queued, not executed inline
            }
        });

        // Step 3: Map hydrated hash entries to DTOs
        List<ItemDto> hydratedItems = new ArrayList<>();
        for (Object result : rawResults) {
            if (result instanceof Map<?, ?> rawMap && !rawMap.isEmpty()) {
                Map<String, Object> map = (Map<String, Object>) rawMap;
                hydratedItems.add(ItemDto.builder()
                        .id((String) map.get("id"))
                        .name((String) map.get("name"))
                        .price(Double.parseDouble(map.get("price").toString()))
                        .views(Long.parseLong(map.get("views").toString()))
                        .build());
            }
        }
        return hydratedItems;
    }
}