package io.github.ooohyg.distributed.cache;

import io.github.ooohyg.distributed.cache.protection.BloomFilterPenetrationGuard;
import io.github.ooohyg.distributed.cache.protection.CachePenetrationGuard;
import io.github.ooohyg.distributed.cache.protection.JitterTtlAvalancheGuard;
import io.github.ooohyg.distributed.cache.protection.PenetrationGuardedReadStrategy;
import io.github.ooohyg.distributed.cache.core.strategy.read.ReadStrategy;
import io.github.ooohyg.distributed.cache.api.ReadContext;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 保护机制模块测试：布隆过滤器防穿透 / TTL 抖动防雪崩 / 穿透守卫读策略装饰器。
 */
class ProtectionModuleTest {

    // ---------- BloomFilterPenetrationGuard ----------

    @Test
    void bloomFilter_markedKey_alwaysExists() {
        BloomFilterPenetrationGuard<String> guard = new BloomFilterPenetrationGuard<>(1000, 0.01);
        guard.markExists("order:1001");
        assertTrue(guard.mightExist("order:1001"), "已插入的 key 绝不假阴性");
    }

    @Test
    void bloomFilter_unmarkedKeys_largelyAbsent_noFalseNegative() {
        BloomFilterPenetrationGuard<String> guard = new BloomFilterPenetrationGuard<>(1000, 0.01);
        for (int i = 0; i < 200; i++) {
            guard.markExists("known-" + i);
        }
        int falsePositives = 0;
        for (int i = 100_000; i < 110_000; i++) {
            if (guard.mightExist("absent-" + i)) {
                falsePositives++;
            }
        }
        // 10000 个未插入 key，误判率应远低于 5%（远小于 1% 理论值，留余量）
        assertTrue(falsePositives < 500, "未插入 key 的误判数应很低，实际=" + falsePositives);
        // 已插入 key 全部能命中
        for (int i = 0; i < 200; i++) {
            assertTrue(guard.mightExist("known-" + i));
        }
    }

    @Test
    void bloomFilter_cardinality_reflectsMarkedBits() {
        BloomFilterPenetrationGuard<String> guard = new BloomFilterPenetrationGuard<>(1000, 0.01);
        int before = guard.cardinality();
        guard.markExists("a");
        guard.markExists("b");
        assertTrue(guard.cardinality() > before, "mark 后位图应有变化");
    }

    // ---------- JitterTtlAvalancheGuard ----------

    @Test
    void jitterTtl_ttlStaysWithinJitterRange() {
        JitterTtlAvalancheGuard<String> guard = new JitterTtlAvalancheGuard<>(0.1);
        Duration base = Duration.ofMinutes(30);
        for (int i = 0; i < 100; i++) {
            long ttl = guard.computeTtl("k" + i, base).toMillis();
            assertTrue(ttl >= 27 * 60_000L && ttl <= 33 * 60_000L,
                    "TTL 应在 [27m, 33m] 内，实际=" + ttl + "ms");
        }
    }

    @Test
    void jitterTtl_producesVariedTtls() {
        JitterTtlAvalancheGuard<String> guard = new JitterTtlAvalancheGuard<>();
        Set<Long> seen = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            seen.add(guard.computeTtl("k" + i, Duration.ofMinutes(30)).toMillis());
        }
        assertTrue(seen.size() > 1, "抖动应产生不同 TTL 值，实际=" + seen.size());
    }

    @Test
    void jitterTtl_invalidRatio_rejected() {
        assertThrows(IllegalArgumentException.class, () -> new JitterTtlAvalancheGuard<>(-0.1));
        assertThrows(IllegalArgumentException.class, () -> new JitterTtlAvalancheGuard<>(1.0));
        assertThrows(IllegalArgumentException.class,
                () -> new JitterTtlAvalancheGuard<>().computeTtl("k", Duration.ZERO));
    }

    // ---------- PenetrationGuardedReadStrategy ----------

    private static class CountingReadStrategy<K, V> implements ReadStrategy<K, V> {
        final AtomicInteger calls = new AtomicInteger();
        @Override public Optional<io.github.ooohyg.distributed.cache.CacheEntry<V>> read(K key, ReadContext<K, V> context) {
            calls.incrementAndGet();
            return Optional.empty();
        }
    }

    @Test
    void penetrationGuard_absentKey_shortCircuitsDelegate() {
        BloomFilterPenetrationGuard<String> guard = new BloomFilterPenetrationGuard<>(1000, 0.01);
        CountingReadStrategy<String, String> delegate = new CountingReadStrategy<>();
        PenetrationGuardedReadStrategy<String, String> strategy =
                new PenetrationGuardedReadStrategy<>(delegate, guard);

        // key 未在布隆过滤器中 → 直接返回 empty，不调 delegate
        assertTrue(strategy.read("unknown", null).isEmpty());
        assertEquals(0, delegate.calls.get(), "布隆过滤器说不存在时不应查任何缓存");

        // 标记后 → 走 delegate
        guard.markExists("known");
        strategy.read("known", null);
        assertEquals(1, delegate.calls.get());
    }
}
