package io.github.ooohyg.distributed.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景二：SingleFlight 并发回源合并验证。
 */
class SingleFlightTest extends BaseCacheTest {

    @Test
    @DisplayName("并发击穿防护：10 个线程并发，只回源 1 次")
    void testSingleFlight() throws Exception {
        source.put("hot-key", "iPhone");
        // 让第一个回源线程停留一段时间，保证其余线程能进入合并窗口
        source.readDelay(150L);

        int threads = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(threads);
        AtomicInteger wrongResults = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    startGate.await(); // 所有线程同时起跑
                    String value = cache.get("hot-key").orElse(null);
                    if (!"iPhone".equals(value)) {
                        wrongResults.incrementAndGet();
                    }
                } catch (Exception e) {
                    wrongResults.incrementAndGet();
                } finally {
                    endGate.countDown();
                }
            });
        }

        startGate.countDown();
        assertTrue(endGate.await(15, TimeUnit.SECONDS), "并发读取应在超时前全部结束");
        pool.shutdown();

        assertEquals(0, wrongResults.get(), "所有线程都应读到 iPhone");
        assertEquals(1, source.readCount(), "并发请求应被 SingleFlight 合并为 1 次回源");
    }

    @Test
    @DisplayName("SingleFlight 合并计数：合并发生的请求数 > 0")
    void testSingleFlightMergeCount() throws Exception {
        source.put("hot-key-2", "Pixel");
        source.readDelay(150L);

        int threads = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    startGate.await();
                    cache.get("hot-key-2");
                } catch (Exception ignored) {
                    // 结果正确性由上一个用例覆盖，这里只关心合并计数
                } finally {
                    endGate.countDown();
                }
            });
        }

        startGate.countDown();
        assertTrue(endGate.await(15, TimeUnit.SECONDS));
        pool.shutdown();

        assertEquals(1, source.readCount(), "同一 key 并发只应回源一次");
        assertTrue(singleFlight.mergeCount() >= 1L,
                "至少应有一次请求被合并，实际 mergeCount=" + singleFlight.mergeCount());
        assertEquals(0, singleFlight.inFlightCount(), "回源结束后 inFlight 应清空");
    }
}
