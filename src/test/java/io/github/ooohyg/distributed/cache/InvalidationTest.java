package io.github.ooohyg.distributed.cache;

import io.github.ooohyg.distributed.cache.L1.ConcurrentHashMapL1Cache;
import io.github.ooohyg.distributed.cache.L2.InMemoryL2Cache;
import io.github.ooohyg.distributed.cache.api.CacheInvalidationEvent;
import io.github.ooohyg.distributed.cache.api.CacheLevel;
import io.github.ooohyg.distributed.cache.core.MultiLevelCache;
import io.github.ooohyg.distributed.cache.failure.NoOpFailurePolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景四：分布式失效广播与版本乱序验证。
 */
class InvalidationTest extends BaseCacheTest {

    @Test
    @DisplayName("写操作广播：其他实例收到后清理本地 L1/L2")
    void testInvalidationBroadcast() {
        // 实例 B 使用独立的 L1/L2，但与实例 A 共享同一条失效总线
        CacheLevel<String, String> bL1 = new ConcurrentHashMapL1Cache<>("B-L1");
        CacheLevel<String, String> bL2 = new InMemoryL2Cache<>("B-L2");
        CountingMetrics bMetrics = new CountingMetrics();
        MultiLevelCache<String, String> b = newCache(bL1, bL2, new NoOpFailurePolicy<>(), bMetrics);

        source.put("user:1", "Old");

        // B 先读到数据，本地 L1/L2 都有缓存
        assertEquals("Old", b.get("user:1").orElse(null));
        assertTrue(bL1.getEntry("user:1").isPresent(), "B 的 L1 应先有缓存");

        // 实例 A 写新数据并发出广播（内存总线同步分发，B 会立即收到）
        cache.put("user:1", "New");

        assertFalse(bL1.getEntry("user:1").isPresent(), "收到广播后 B 的 L1 应该被清空");
        assertFalse(bL2.getEntry("user:1").isPresent(), "收到广播后 B 的 L2 应该被清空");
        assertEquals(1, bMetrics.invalidationCount(), "B 应记录一次失效事件处理");
    }

    @Test
    @DisplayName("写操作后本地缓存失效，下一次读取回源拿到新版本")
    void testLocalCacheEvictedAfterPut() {
        source.put("user:1", "Old");
        assertEquals("Old", cache.get("user:1").orElse(null));
        assertTrue(l1.getEntry("user:1").isPresent());

        // 本实例写新数据：写源 -> 删 L1/L2 -> 广播（自己也会收到）
        cache.put("user:1", "New");

        assertFalse(l1.getEntry("user:1").isPresent(), "写后 L1 应该被清空");
        assertFalse(l2.getEntry("user:1").isPresent(), "写后 L2 应该被清空");
        assertEquals(1, metrics.invalidationCount(), "本实例应处理到一次失效事件");
        assertEquals("New", cache.get("user:1").orElse(null), "回源应拿到新写入的值");
    }

    @Test
    @DisplayName("版本乱序防护：收到旧版本的事件直接丢弃")
    void testStaleEventIgnored() {
        // 本地缓存了 v5 版本的新数据
        l1.putEntry("user:1", CacheEntry.from(new VersionedValue<>("Newest", 5L)));

        // 伪造一个网络延迟晚到的旧事件 (v3)
        CacheInvalidationEvent staleEvent = new CacheInvalidationEvent(
                "event-123", "user:1", 3L, EventType.INVALIDATE, System.currentTimeMillis());

        // 让编排器直接处理这个事件
        cache.onEvent(staleEvent);

        // 断言：本地 v5 的数据不应该被 v3 的旧事件删掉
        assertTrue(l1.getEntry("user:1").isPresent(), "旧版本事件不能误删新版本缓存");
        assertEquals("Newest", l1.getEntry("user:1").orElseThrow().value());
        assertEquals(1, metrics.staleEventIgnoredCount(), "指标应记录一次乱序丢弃");
        assertEquals(0, metrics.invalidationCount(), "被丢弃的事件不应计入生效失效");
    }

    @Test
    @DisplayName("版本更新的事件正常生效：v6 事件可清除本地 v5 缓存")
    void testNewerEventApplied() {
        l1.putEntry("user:1", CacheEntry.from(new VersionedValue<>("Newest", 5L)));

        cache.onEvent(new CacheInvalidationEvent("event-456", "user:1", 6L,
                EventType.INVALIDATE, System.currentTimeMillis()));

        assertFalse(l1.getEntry("user:1").isPresent(), "更高版本的事件应清空本地缓存");
        assertEquals(0, metrics.staleEventIgnoredCount());
        assertEquals(1, metrics.invalidationCount());
    }
}
