package io.github.ooohyg.distributed.cache;

import io.github.ooohyg.distributed.cache.L1.CaffeineL1Cache;
import io.github.ooohyg.distributed.cache.L1.ConcurrentHashMapL1Cache;
import io.github.ooohyg.distributed.cache.L2.InMemoryL2Cache;
import io.github.ooohyg.distributed.cache.api.CacheLevel;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * L1 / L2 缓存层模块测试：
 * 1) ConcurrentHashMapL1Cache：基本存取、惰性过期、evict/clear；
 * 2) CaffeineL1Cache：L1 TTL 截断（min(L1 TTL, 原 entry 剩余时间)）、无 TTL 时用 L1 TTL；
 * 3) InMemoryL2Cache：基本存取、故障注入（不可用时 get 空 / put 忽略）、evict 在故障时仍生效。
 */
class CacheLevelModuleTest {

    // ---------- ConcurrentHashMapL1Cache ----------

    @Test
    void concurrentL1_putGetEvictClear_workAsExpected() {
        CacheLevel<String, String> l1 = new ConcurrentHashMapL1Cache<>();
        CacheEntry<String> entry = CacheEntry.from(new VersionedValue<>("v1", 1L), Duration.ofMinutes(10));

        assertTrue(l1.getEntry("k").isEmpty());
        l1.putEntry("k", entry);
        assertEquals("v1", l1.getEntry("k").orElseThrow().value());
        assertEquals(1L, l1.getEntry("k").orElseThrow().version());

        l1.evict("k");
        assertTrue(l1.getEntry("k").isEmpty());

        l1.putEntry("k", entry);
        l1.clear();
        assertTrue(l1.getEntry("k").isEmpty());
        assertEquals(0, ((ConcurrentHashMapL1Cache<String, String>) l1).size());
    }

    @Test
    void concurrentL1_lazyExpiresPhysicalExpiredEntry() {
        ConcurrentHashMapL1Cache<String, String> l1 = new ConcurrentHashMapL1Cache<>();
        // 物理过期时间设为过去：直接放一个过期 entry
        CacheEntry<String> expired = new CacheEntry<>("old", 1L, false, 0L,
                System.currentTimeMillis() - 1);
        l1.putEntry("k", expired);

        assertTrue(l1.getEntry("k").isEmpty(), "物理过期条目读取时应被惰性删除");
        assertEquals(0, l1.size());
    }

    // ---------- CaffeineL1Cache ----------

    @Test
    void caffeineL1_shortensTtlToMinOfL1AndOriginal() throws InterruptedException {
        // L1 TTL 500ms，业务 TTL 10s
        CaffeineL1Cache<String, String> l1 = new CaffeineL1Cache<>(
                "short-l1", 100, Duration.ofHours(1), Duration.ofMillis(500));

        CacheEntry<String> entry = CacheEntry.from(new VersionedValue<>("v1", 1L), Duration.ofSeconds(10));
        l1.putEntry("k", entry);
        assertTrue(l1.getEntry("k").isPresent());

        Thread.sleep(700);   // 超过 L1 TTL 但远小于业务 TTL
        assertTrue(l1.getEntry("k").isEmpty(), "L1 回填 TTL 应截断到 L1 自己的 TTL");
    }

    @Test
    void caffeineL1_noExpiryEntry_getsL1TtlFallback() throws InterruptedException {
        CaffeineL1Cache<String, String> l1 = new CaffeineL1Cache<>(
                "fallback-l1", 100, Duration.ofHours(1), Duration.ofMillis(300));

        CacheEntry<String> noTtl = CacheEntry.from(new VersionedValue<>("v1", 1L));   // 永不过期
        l1.putEntry("k", noTtl);
        assertTrue(l1.getEntry("k").isPresent());

        Thread.sleep(500);
        assertTrue(l1.getEntry("k").isEmpty(), "无业务 TTL 的条目应按 L1 fallback TTL 过期");
    }

    @Test
    void caffeineL1_evictAndClear_work() {
        CaffeineL1Cache<String, String> l1 = new CaffeineL1Cache<>();
        CacheEntry<String> entry = CacheEntry.from(new VersionedValue<>("v1", 1L), Duration.ofMinutes(10));
        l1.putEntry("a", entry);
        l1.putEntry("b", entry);
        l1.evict("a");
        assertTrue(l1.getEntry("a").isEmpty());
        assertTrue(l1.getEntry("b").isPresent());

        l1.clear();
        assertTrue(l1.getEntry("b").isEmpty());
    }

    // ---------- InMemoryL2Cache ----------

    @Test
    void inMemoryL2_basicPutGet_works() {
        InMemoryL2Cache<String, String> l2 = new InMemoryL2Cache<>();
        CacheEntry<String> entry = CacheEntry.from(new VersionedValue<>("v1", 1L), Duration.ofMinutes(10));

        assertTrue(l2.getEntry("k").isEmpty());
        l2.putEntry("k", entry);
        assertEquals("v1", l2.getEntry("k").orElseThrow().value());
        assertEquals(1, l2.size());
    }

    @Test
    void inMemoryL2_unavailable_returnsEmptyAndIgnoresPuts_butEvictStillWorks() {
        InMemoryL2Cache<String, String> l2 = new InMemoryL2Cache<>();
        CacheEntry<String> entry = CacheEntry.from(new VersionedValue<>("v1", 1L), Duration.ofMinutes(10));

        l2.putEntry("k", entry);
        assertEquals("v1", l2.getEntry("k").orElseThrow().value());

        // 故障注入：模拟 Redis 挂掉
        l2.setAvailable(false);
        assertTrue(l2.getEntry("k").isEmpty(), "不可用时读应返回空");
        l2.putEntry("k2", entry);   // 写入被忽略
        assertEquals(1, l2.size(), "不可用时 put 应被忽略");

        // evict 即使在不可用时也生效（本地 map 不依赖网络）
        l2.evict("k");
        l2.setAvailable(true);
        assertEquals(0, l2.size(), "故障期间的 evict 应生效");
        assertTrue(l2.getEntry("k").isEmpty());
    }

    @Test
    void inMemoryL2_lazyExpiresPhysicalExpiredEntry() {
        InMemoryL2Cache<String, String> l2 = new InMemoryL2Cache<>();
        CacheEntry<String> expired = new CacheEntry<>("old", 1L, false, 0L,
                System.currentTimeMillis() - 1);
        l2.putEntry("k", expired);
        assertTrue(l2.getEntry("k").isEmpty());
        assertEquals(0, l2.size());
    }
}
