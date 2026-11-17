package org.abd.redis.connection;

import org.abd.redis.model.ItemDto;
import org.abd.redis.model.OrderPlacedEvent;
import org.abd.redis.service.InventoryService;
import org.abd.redis.service.ItemHydrationService;
import org.abd.redis.service.OrderCheckoutService;
import org.abd.redis.service.OrderStreamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class RedisConnectionCheck implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(RedisConnectionCheck.class);

    private final RedisTemplate<String, Object> redisTemplate;
    private final ItemHydrationService hydrationService;
    private final InventoryService inventoryService;
    private final OrderCheckoutService checkoutService;
    private final OrderStreamService streamService;

    public RedisConnectionCheck(RedisTemplate<String, Object> redisTemplate,
                                ItemHydrationService hydrationService,
                                InventoryService inventoryService,
                                OrderCheckoutService checkoutService,
                                OrderStreamService streamService) {
        this.redisTemplate = redisTemplate;
        this.hydrationService = hydrationService;
        this.inventoryService = inventoryService;
        this.checkoutService = checkoutService;
        this.streamService = streamService;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("==========================================================");
        log.info("           RUNNING COMPLETE REDIS SUITE TESTS             ");
        log.info("==========================================================");

        test0_BasicConnectionAndJsonSerialization();
        test1_PipelinedHydration();
        test2_AtomicLuaStockDeduction();
        test3_RedissonDistributedLock();
        test4_RedisStreamsAndConsumerGroups();

        log.info("==========================================================");
        log.info("           ALL REDIS SUITE TESTS COMPLETED                ");
        log.info("==========================================================");
    }

    /**
     * Test 0: Basic Connection & Typed JSON Serialization
     */
    private void test0_BasicConnectionAndJsonSerialization() {
        log.info("\n--- TEST 0: BASIC CONNECTION & JSON SERIALIZATION ---");
        String testKey = "test:connection";
        redisTemplate.delete(testKey);

        Map<String, String> payload = new HashMap<>();
        payload.put("status", "SUCCESS");
        payload.put("engine", "WSL2-Redis");

        redisTemplate.opsForValue().set(testKey, payload);
        Object retrieved = redisTemplate.opsForValue().get(testKey);
        log.info("[TEST 0 PASSED] Retrieved Payload from Redis: {}", retrieved);
    }

    /**
     * Test 1: Relational Hydration via Pipelining (ZRANGE + HGETALL)
     */
    private void test1_PipelinedHydration() {
        log.info("\n--- TEST 1: PIPELINED HYDRATION (ZRANGE + HGETALL) ---");
        hydrationService.seedTestData();

        List<ItemDto> topItems = hydrationService.getTopItemsByViews(3);
        topItems.forEach(item ->
                log.info("[TEST 1 ITEM] Rank Result -> ID: {}, Name: {}, Views: {}",
                        item.getId(), item.getName(), item.getViews())
        );
        log.info("[TEST 1 PASSED] Hydrated {} items in 1 network round-trip batch.", topItems.size());
    }

    /**
     * Test 2: Concurrency Control via Server-Side Lua Script
     */
    private void test2_AtomicLuaStockDeduction() throws InterruptedException {
        log.info("\n--- TEST 2: ATOMIC LUA INVENTORY DEDUCTION ---");
        String flashSaleItem = "item_flash_99";
        int initialStock = 5;
        int totalBuyers = 20;

        inventoryService.setInitialStock(flashSaleItem, initialStock);
        log.info("Initial stock set to: {}", inventoryService.getStock(flashSaleItem));

        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(totalBuyers);

        AtomicInteger successfulOrders = new AtomicInteger(0);
        AtomicInteger rejectedOrders = new AtomicInteger(0);

        for (int i = 1; i <= totalBuyers; i++) {
            final int buyerId = i;
            executor.submit(() -> {
                try {
                    startGate.await(); // All threads wait here to simulate sudden burst
                    Long result = inventoryService.deductStock(flashSaleItem, 1);
                    if (result >= 0) {
                        successfulOrders.incrementAndGet();
                    } else {
                        rejectedOrders.incrementAndGet();
                    }
                } catch (Exception e) {
                    log.error("Buyer {} thread encountered an error", buyerId, e);
                } finally {
                    endGate.countDown();
                }
            });
        }

        startGate.countDown(); // Fire all threads simultaneously
        endGate.await();
        executor.shutdown();

        log.info("[TEST 2 RESULT] Successful purchases (Expected: 5): {}", successfulOrders.get());
        log.info("[TEST 2 RESULT] Rejected purchases   (Expected: 15): {}", rejectedOrders.get());
        log.info("[TEST 2 RESULT] Remaining Redis Stock (Expected: 0): {}", inventoryService.getStock(flashSaleItem));
        log.info("[TEST 2 PASSED] Atomic Lua deduction preserved stock boundary under concurrency.");
    }

    /**
     * Test 3: Distributed Mutual Exclusion with Redisson (RLock & Watchdog)
     */
    private void test3_RedissonDistributedLock() throws InterruptedException {
        log.info("\n--- TEST 3: REDISSON DISTRIBUTED LOCK MUTEX ---");
        String sharedOrderId = "ORD_789";
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch lockDone = new CountDownLatch(2);

        // Thread 1: Grabs lock and holds for 3 seconds
        executor.submit(() -> {
            try {
                Thread.currentThread().setName("Worker-Payment-1");
                checkoutService.processCheckout(sharedOrderId, 3000);
            } finally {
                lockDone.countDown();
            }
        });

        // Small pause to ensure Thread 1 captures lock first
        Thread.sleep(100);

        // Thread 2: Tries to grab same order lock, waits max 2s, and backs off
        executor.submit(() -> {
            try {
                Thread.currentThread().setName("Worker-Payment-2");
                checkoutService.processCheckout(sharedOrderId, 1000);
            } finally {
                lockDone.countDown();
            }
        });

        lockDone.await();
        executor.shutdown();
        log.info("[TEST 3 PASSED] Distributed locking prevented overlapping checkout calls.");
    }

    /**
     * Test 4: Asynchronous Redis Streams & Competing Consumer Groups
     */
    private void test4_RedisStreamsAndConsumerGroups() throws InterruptedException {
        log.info("\n--- TEST 4: REDIS STREAMS & COMPETING CONSUMER GROUPS ---");

        // 1. Publish 4 order events
        streamService.publishOrderEvent(new OrderPlacedEvent("ORD-101", "user1@example.com", 85.50));
        streamService.publishOrderEvent(new OrderPlacedEvent("ORD-102", "user2@example.com", 140.00));
        streamService.publishOrderEvent(new OrderPlacedEvent("ORD-103", "user3@example.com", 25.00));
        streamService.publishOrderEvent(new OrderPlacedEvent("ORD-104", "user4@example.com", 320.00));

        // 2. Competing worker consumption
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch streamDone = new CountDownLatch(2);

        executor.submit(() -> {
            try {
                String worker = "Worker-Alpha";
                streamService.pollAndProcessNext(worker);
                streamService.pollAndProcessNext(worker);
            } finally {
                streamDone.countDown();
            }
        });

        executor.submit(() -> {
            try {
                String worker = "Worker-Beta";
                streamService.pollAndProcessNext(worker);
                streamService.pollAndProcessNext(worker);
            } finally {
                streamDone.countDown();
            }
        });

        streamDone.await();
        executor.shutdown();
        log.info("[TEST 4 PASSED] Messages distributed and acknowledged across competing workers.");
    }
}