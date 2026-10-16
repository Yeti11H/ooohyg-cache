package io.github.ooohyg.distributed.cache;

import io.github.ooohyg.distributed.cache.core.SingleFlight;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SingleFlight 并发回源合并器模块测试：
 * 1) 同一 key 并发请求只执行一次 loader，其余等待合并结果；
 * 2) loader 失败时所有等待者拿到原始异常；
 * 3) 等待超时（loader 卡死）时等待者抛超时异常；
 * 4) 不同 key 互不阻塞。
 *
 * <p>并发用例注意：线程池线程数必须 >= 请求数，且用 latch 同时放行，
 * 否则请求会分多批执行（每批一个 leader），合并次数达不到预期。
 */
class SingleFlightModuleTest {

    @Test
    void concurrentSameKey_executesLoaderOnce_andAllGetSameResult() throws Exception {
        SingleFlight<String, String> sf = new SingleFlight<>();
        AtomicInteger loads = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);

        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<VersionedValue<String>>> futures = new CopyOnWriteArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> sf.execute("k", () -> {
                    startLatch.await();
                    loads.incrementAndGet();
                    Thread.sleep(80);
                    return new VersionedValue<>("db-v", 1L);
                })));
            }
            startLatch.countDown();

            for (Future<VersionedValue<String>> f : futures) {
                VersionedValue<String> v = f.get(5, TimeUnit.SECONDS);
                assertEquals("db-v", v.value());
                assertEquals(1L, v.version());
            }
            assertEquals(1, loads.get(), "loader 只应执行一次");
            assertEquals(threads - 1, sf.mergeCount(), "其余请求被合并");
            assertEquals(0, sf.inFlightCount(), "结束后 inFlight 应清空");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void loaderFailure_propagatesToAllWaiters() throws Exception {
        SingleFlight<String, String> sf = new SingleFlight<>();
        CountDownLatch startLatch = new CountDownLatch(1);

        int threads = 5;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> futures = new CopyOnWriteArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    try {
                        sf.execute("bad", () -> {
                            startLatch.await();
                            Thread.sleep(80);
                            throw new IllegalStateException("source down");
                        });
                        return "unexpected";
                    } catch (Exception e) {
                        return e.getClass().getSimpleName() + ":" + e.getMessage();
                    }
                }));
            }
            startLatch.countDown();

            for (Future<?> f : futures) {
                assertEquals("IllegalStateException:source down", f.get(5, TimeUnit.SECONDS));
            }
            assertEquals(threads - 1, sf.mergeCount(), "失败也应合并等待者");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void loaderHangs_waiterTimesOut() throws Exception {
        SingleFlight<String, String> sf = new SingleFlight<>(Duration.ofMillis(200));
        CountDownLatch loaderDone = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<VersionedValue<String>> first = pool.submit(() -> sf.execute("slow", () -> {
                try {
                    Thread.sleep(2_000);
                    return new VersionedValue<>("v", 1L);
                } finally {
                    loaderDone.countDown();
                }
            }));
            Thread.sleep(100);   // 保证 first 已经进入 loader

            assertThrows(RuntimeException.class, () -> sf.execute("slow", () -> null),
                    "等待者超过 waitTimeout 应抛异常");

            // loader 最终仍会完成并清理 Map
            loaderDone.await(3, TimeUnit.SECONDS);
            first.get(1, TimeUnit.SECONDS);
            assertEquals(0, sf.inFlightCount(), "loader 完成后 inFlight 应清空");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void differentKeys_doNotBlockEachOther() throws Exception {
        SingleFlight<String, String> sf = new SingleFlight<>();
        CountDownLatch key1Latch = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<VersionedValue<String>> f1 = pool.submit(() -> sf.execute("k1", () -> {
                key1Latch.await(2, TimeUnit.SECONDS);
                return new VersionedValue<>("v1", 1L);
            }));

            Thread.sleep(100);
            // k2 不等 k1 的锁，直接执行
            VersionedValue<String> v2 = sf.execute("k2", () -> new VersionedValue<>("v2", 1L));
            assertEquals("v2", v2.value());
            key1Latch.countDown();
            assertEquals("v1", f1.get(3, TimeUnit.SECONDS).value());
            assertEquals(0, sf.inFlightCount());
        } finally {
            pool.shutdownNow();
        }
    }
}
