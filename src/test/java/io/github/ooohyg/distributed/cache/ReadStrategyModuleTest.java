package io.github.ooohyg.distributed.cache;

import io.github.ooohyg.distributed.cache.api.ReadContext;
import io.github.ooohyg.distributed.cache.core.SingleFlight;
import io.github.ooohyg.distributed.cache.core.strategy.read.CacheAsideReadStrategy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 读策略模块测试：CacheAsideReadStrategy 的
 * L1 命中 / L2 命中回填 / L3 回源回填 / 空值占位 / 失败降级 / SingleFlight 联动。
 *
 * <p>使用自定义 {@link RecordingReadContext} 精确控制各层数据，
 * 不依赖 MultiLevelCache 的默认 TTL 与指标实现。
 */
class ReadStrategyModuleTest {

    static class RecordingReadContext<K, V> implements ReadContext<K, V> {
        final Map<K, CacheEntry<V>> l1 = new ConcurrentHashMap<>();
        final Map<K, CacheEntry<V>> l2 = new ConcurrentHashMap<>();
        final Map<K, VersionedValue<V>> source = new ConcurrentHashMap<>();
        final AtomicInteger loads = new AtomicInteger();
        final CopyOnWriteArrayList<String> hits = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<String> misses = new CopyOnWriteArrayList<>();
        volatile Exception sourceError;
        volatile boolean returnNull;
        /** 回源耗时，用于给 SingleFlight 制造并发合并窗口（默认 0 表示不延迟）。 */
        volatile long loadDelayMillis;
        final Duration defaultTtl = Duration.ofMinutes(30);
        final Duration nullTtl = Duration.ofMinutes(1);

        @Override public Duration defaultTtl() { return defaultTtl; }
        @Override public Duration nullValueTtl() { return nullTtl; }

        @Override public Optional<CacheEntry<V>> readL1(K key) { return Optional.ofNullable(l1.get(key)); }
        @Override public Optional<CacheEntry<V>> readL2(K key) {
            // 模拟 MultiLevelCache 的 ReadContext：逻辑过期视为 miss（保留在 l2 供失败降级）
            CacheEntry<V> e = l2.get(key);
            if (e == null) {
                return Optional.empty();
            }
            if (e.isLogicalExpired(System.currentTimeMillis())) {
                return Optional.empty();
            }
            return Optional.of(e);
        }
        @Override public Optional<CacheEntry<V>> peekStale(K key) {
            CacheEntry<V> e = l1.get(key);
            return Optional.ofNullable(e != null ? e : l2.get(key));
        }
        @Override public void writeL1(K key, CacheEntry<V> entry) { l1.put(key, entry); }
        @Override public void writeL2(K key, CacheEntry<V> entry) { l2.put(key, entry); }

        @Override public VersionedValue<V> loadFromSource(K key) throws Exception {
            loads.incrementAndGet();
            if (loadDelayMillis > 0L) {
                Thread.sleep(loadDelayMillis);
            }
            if (returnNull) {
                return null;
            }
            if (sourceError != null) {
                throw sourceError;
            }
            return source.get(key);
        }

        @Override public Optional<CacheEntry<V>> handleLoadFailure(K key, Throwable error) {
            // 模拟 NoOpFailurePolicy：无降级值，返回 stale（若有）
            return peekStale(key);
        }

        @Override public void recordHit(String level) { hits.add(level); }
        @Override public void recordMiss(String level) { misses.add(level); }
        @Override public void recordLoadTime(Duration duration) { }
    }

    private CacheEntry<String> freshEntry(String value, long version) {
        return CacheEntry.from(new VersionedValue<>(value, version), Duration.ofMinutes(30));
    }

    @Test
    void l1Hit_returnsWithoutTouchingL2OrSource() {
        RecordingReadContext<String, String> ctx = new RecordingReadContext<>();
        ctx.l1.put("k", freshEntry("l1-value", 1L));
        CacheAsideReadStrategy<String, String> strategy = new CacheAsideReadStrategy<>();

        Optional<CacheEntry<String>> result = strategy.read("k", ctx);

        assertTrue(result.isPresent());
        assertEquals("l1-value", result.get().value());
        assertEquals(0, ctx.loads.get(), "L1 命中不应回源");
        assertTrue(ctx.hits.contains("L1"));
        assertTrue(ctx.misses.isEmpty());
    }

    @Test
    void l2Hit_backfillsL1_withoutSource() {
        RecordingReadContext<String, String> ctx = new RecordingReadContext<>();
        ctx.l2.put("k", freshEntry("l2-value", 2L));
        CacheAsideReadStrategy<String, String> strategy = new CacheAsideReadStrategy<>();

        Optional<CacheEntry<String>> result = strategy.read("k", ctx);

        assertTrue(result.isPresent());
        assertEquals("l2-value", result.get().value());
        assertEquals(0, ctx.loads.get(), "L2 命中不应回源");
        assertTrue(ctx.hits.contains("L2"));
        assertEquals("l2-value", ctx.l1.get("k").value(), "L2 命中应回填 L1");
    }

    @Test
    void l3Miss_loadsSource_andBackfillsBothLevels() {
        RecordingReadContext<String, String> ctx = new RecordingReadContext<>();
        ctx.source.put("k", new VersionedValue<>("db-value", 3L));
        CacheAsideReadStrategy<String, String> strategy = new CacheAsideReadStrategy<>();

        Optional<CacheEntry<String>> result = strategy.read("k", ctx);

        assertTrue(result.isPresent());
        assertEquals("db-value", result.get().value());
        assertEquals(3L, result.get().version());
        assertEquals(1, ctx.loads.get());
        assertEquals("db-value", ctx.l2.get("k").value(), "应回填 L2");
        assertEquals("db-value", ctx.l1.get("k").value(), "应回填 L1");
        assertTrue(ctx.misses.contains("L1"));
        assertTrue(ctx.misses.contains("L2"));
    }

    @Test
    void nullSource_writesNullPlaceholder_andSkipsReRead() {
        RecordingReadContext<String, String> ctx = new RecordingReadContext<>();
        ctx.returnNull = true;
        CacheAsideReadStrategy<String, String> strategy = new CacheAsideReadStrategy<>();

        Optional<CacheEntry<String>> result = strategy.read("k", ctx);

        assertTrue(result.isEmpty(), "L3 明确不存在时返回 empty");
        assertTrue(ctx.l1.containsKey("k"));
        assertTrue(ctx.l2.containsKey("k"));
        assertTrue(ctx.l1.get("k").nullValue(), "回填的是空值占位符");
        assertEquals(1, ctx.loads.get());
    }

    @Test
    void sourceFailure_noStale_returnsEmpty() {
        RecordingReadContext<String, String> ctx = new RecordingReadContext<>();
        ctx.sourceError = new IllegalStateException("source down");
        CacheAsideReadStrategy<String, String> strategy = new CacheAsideReadStrategy<>();

        Optional<CacheEntry<String>> result = strategy.read("k", ctx);

        assertTrue(result.isEmpty(), "无降级值且无 stale 时返回 empty");
        assertEquals(1, ctx.loads.get());
    }

    @Test
    void sourceFailure_withStale_returnsStaleValue() {
        RecordingReadContext<String, String> ctx = new RecordingReadContext<>();
        ctx.sourceError = new IllegalStateException("source down");
        // L2 里有逻辑过期但物理未过期的旧值（缓存层视为 miss 但保留供降级）
        ctx.l2.put("k", new CacheEntry<>("stale-value", 1L, false,
                System.currentTimeMillis() - 1000, 0L));
        CacheAsideReadStrategy<String, String> strategy = new CacheAsideReadStrategy<>();

        Optional<CacheEntry<String>> result = strategy.read("k", ctx);

        assertTrue(result.isPresent());
        assertEquals("stale-value", result.get().value(), "L3 故障时返回旧值");
        assertEquals(1, ctx.loads.get(), "逻辑过期应触发回源");
    }

    @Test
    void singleFlight_coalescesConcurrentLoads() throws Exception {
        RecordingReadContext<String, String> ctx = new RecordingReadContext<>();
        ctx.source.put("k", new VersionedValue<>("db-value", 4L));
        // 给回源加 100ms 延迟，确保 4 个线程落在同一合并窗口内（否则线程串行时断言必然抖动）
        ctx.loadDelayMillis = 100L;
        SingleFlight<String, String> sf = new SingleFlight<>();
        CacheAsideReadStrategy<String, String> strategy = new CacheAsideReadStrategy<>(sf);

        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<Optional<CacheEntry<String>>>> futures = new CopyOnWriteArrayList<>();
            for (int i = 0; i < 4; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return strategy.read("k", ctx);
                }));
            }
            start.countDown();
            for (Future<Optional<CacheEntry<String>>> f : futures) {
                assertTrue(f.get(5, TimeUnit.SECONDS).isPresent());
            }
            assertEquals(1, ctx.loads.get(), "并发读同一 key 应只回源一次");
        } finally {
            pool.shutdownNow();
        }
    }
}
