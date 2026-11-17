package org.abd.redis.service;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class OrderCheckoutService {

    private static final Logger log = LoggerFactory.getLogger(OrderCheckoutService.class);
    private final RedissonClient redissonClient;

    public OrderCheckoutService(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    /**
     * Executes order checkout protected by a distributed lock.
     *
     * @param orderId       Target resource identifier
     * @param simulatedWorkMs Duration to simulate external payment I/O
     * @return true if acquired and processed; false if lock timed out
     */
    public boolean processCheckout(String orderId, long simulatedWorkMs) {
        String lockKey = "lock:order:" + orderId;
        RLock lock = redissonClient.getLock(lockKey);

        try {
            // tryLock(waitTime, unit):
            // - waitTime: How long to wait for the lock before giving up (e.g. 2s)
            // - Watchdog enabled: Since leaseTime is omitted, watchdog renews lock every 10s
            boolean isLocked = lock.tryLock(2, TimeUnit.SECONDS);

            if (!isLocked) {
                log.warn("[Thread {}] Failed to acquire lock for order: {}. Request rejected.",
                        Thread.currentThread().getName(), orderId);
                return false;
            }

            try {
                log.info("[Thread {}] Lock acquired for order: {}. Processing payment...",
                        Thread.currentThread().getName(), orderId);

                // Simulate heavy external work (Stripe API / DB writes)
                Thread.sleep(simulatedWorkMs);

                log.info("[Thread {}] Payment successful for order: {}.",
                        Thread.currentThread().getName(), orderId);
                return true;
            } finally {
                // Ensure only the thread holding the lock releases it
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                    log.info("[Thread {}] Lock released cleanly for order: {}.",
                            Thread.currentThread().getName(), orderId);
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Lock acquisition interrupted", e);
            return false;
        }
    }
}