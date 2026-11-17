package org.abd.redis.service;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Service
public class InventoryService {

    private final RedisTemplate<String, Object> redisTemplate;
    private final DefaultRedisScript<Long> deductStockScript;

    public InventoryService(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;

        // Initialize and compile the Lua script
        this.deductStockScript = new DefaultRedisScript<>();
        this.deductStockScript.setScriptSource(
                new ResourceScriptSource(new ClassPathResource("scripts/deduct_stock.lua"))
        );
        this.deductStockScript.setResultType(Long.class);
    }

    /**
     * Initializes stock for a given item.
     */
    public void setInitialStock(String itemId, long quantity) {
        redisTemplate.opsForValue().set("stock:" + itemId, quantity);
    }

    /**
     * Atomically checks and deducts stock using Lua.
     *
     * @return Remaining stock if deduction succeeded;
     *         -1 if out of stock / insufficient quantity;
     *         -2 if key does not exist.
     */
    public Long deductStock(String itemId, long quantityToDeduct) {
        List<String> keys = Collections.singletonList("stock:" + itemId);

        // Execute Lua script atomically on Redis server
        return redisTemplate.execute(
                deductStockScript,
                keys,
                quantityToDeduct
        );
    }

    public Long getStock(String itemId) {
        Object val = redisTemplate.opsForValue().get("stock:" + itemId);
        if (val == null) return 0L;
        return Long.parseLong(val.toString());
    }
}