package io.github.ooohyg.distributed.cache;

import io.github.ooohyg.distributed.cache.L1.ConcurrentHashMapL1Cache;
import io.github.ooohyg.distributed.cache.L2.InMemoryL2Cache;
import io.github.ooohyg.distributed.cache.api.CacheLevel;
import io.github.ooohyg.distributed.cache.core.MultiLevelCache;
import io.github.ooohyg.distributed.cache.failure.FailurePolicy;
import io.github.ooohyg.distributed.cache.failure.NoOpFailurePolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景五：容错降级（源故障时返回旧值）。
 *
 * <p>原文档此用例只写了思路（"详细测试代码略"），这里按真实编排器语义补全三条路径：
 * <ol>
 *   <li>FailurePolicy 提供降级值 → 直接返回该值；</li>
 *   <li>FailurePolicy 拒绝降级（默认 NoOp）→ 编排器用 peekStale 拿缓存旧值兜底；</li>
 *   <li>既无策略值也无旧值 → 返回 empty，不向业务抛异常、也不返回脏数据。</li>
 * </ol>
 */
class FailurePolicyTest extends BaseCacheTest {

    private static final RuntimeException DB_DOWN = new RuntimeException("L3 database is down");

    @Test
    @DisplayName("降级容错：DB 挂掉时，FailurePolicy 返回旧值")
    void testFallbackByFailurePolicy() {
        // 1. 先正常读一次，让缓存有值
        source.put("user:1", "OldData");
        MultiLevelCache<String, String> policyCache = newCache(l1, l2,
                new FailurePolicy<String, String>() {
                    @Override
                    public Optional<String> onLoadFailure(String key, Throwable error) {
                        return Optional.of("OldData"); // 业务允许返回旧值
                    }

                    @Override
                    public Optional<String> onLevelFailure(String key, String levelName, Throwable error) {
                        return Optional.empty();
                    }
                }, metrics);
        assertEquals("OldData", policyCache.get("user:1").orElse(null));
        assertEquals(1, source.readCount());

        // 2. 清空两级缓存，制造"只能靠降级兜底"的局面
        l1.clear();
        l2.clear();

        // 3. 模拟数据库挂掉
        source.failWith(DB_DOWN);
        source.resetReadCount();

        // 4. 核心断言：loadFromSource 抛异常时，返回值应为 OldData
        Optional<String> degraded = policyCache.get("user:1");
        assertTrue(degraded.isPresent(), "源故障时不应向业务抛异常");
        assertEquals("OldData", degraded.orElse(null), "应由 FailurePolicy 返回旧值兜底");
        assertEquals(1, source.readCount(), "应确认真实访问过故障的源");
        // 说明：真实源码的 MultiLevelCache 仅调用 recordLoadTime，未调用 CacheMetrics.recordLoadFailure，
        // 故这里断言"失败路径留下一次加载耗时"，recordLoadFailure 未接入的差异见测试报告。
        assertEquals(2, metrics.loadTimeCount(), "成功读一次 + 失败一次，各记录一次耗时");
    }

    @Test
    @DisplayName("默认降级策略：源故障时编排器用 peekStale 返回缓存旧值")
    void testFallbackToStaleValue() {
        // 数据源先有值，L1 中已存在旧条目（模拟"过期但仍可读"的旧值）
        source.put("user:1", "OldData");
        ConcurrentHashMapL1Cache<String, String> staleL1 = new ConcurrentHashMapL1Cache<>("stale-L1");
        staleL1.putEntry("user:1", CacheEntry.from(new VersionedValue<>("OldData", 1L)));
        InMemoryL2Cache<String, String> emptyL2 = new InMemoryL2Cache<>("empty-L2");

        // 第一次 getEntry 返回空（模拟缓存层瞬时读失败），之后返回真实旧条目；
        // 这样 L1/L2 判定为 miss 会走到 L3，源故障后 peekStale 才能取到旧值。
        CacheLevel<String, String> flakyL1 = new FirstReadMissLevel(staleL1);
        MultiLevelCache<String, String> staleCache = newCache(flakyL1, emptyL2,
                new NoOpFailurePolicy<>(), metrics);

        source.failWith(DB_DOWN);

        Optional<String> degraded = staleCache.get("user:1");

        assertTrue(degraded.isPresent(), "有旧值时不应返回 empty");
        assertEquals("OldData", degraded.orElse(null), "源故障时应回退到缓存中的旧值");
        assertEquals(1, source.readCount(), "应先尝试过回源");
        assertEquals(1, metrics.loadTimeCount(), "失败路径应留下一次加载耗时指标");
    }

    @Test
    @DisplayName("无旧值兜底：源故障且缓存全空时返回 empty，不抛异常")
    void testNoStaleValueReturnsEmpty() {
        MultiLevelCache<String, String> emptyCache = newCache(
                new ConcurrentHashMapL1Cache<>("empty-L1"),
                new InMemoryL2Cache<>("empty-L2"),
                new NoOpFailurePolicy<>(), metrics);
        source.failWith(DB_DOWN);

        Optional<String> result = assertDoesNotThrow(() -> emptyCache.get("user:1"));
        assertTrue(result.isEmpty(), "无旧值时应返回 empty 而不是脏数据");
        assertEquals(1, metrics.loadTimeCount(), "失败路径应留下一次加载耗时指标");
    }

    @Test
    @DisplayName("源恢复后自动回到正常链路")
    void testRecoveryAfterSourceBack() {
        source.put("user:1", "OldData");
        cache.get("user:1");

        l1.evict("user:1");
        l2.evict("user:1");
        source.failWith(DB_DOWN);
        assertTrue(cache.get("user:1").isEmpty(), "源故障且无旧值时返回 empty");

        source.recover();
        assertEquals("OldData", cache.get("user:1").orElse(null), "源恢复后应能正常回源");
        assertEquals(3, metrics.loadTimeCount(), "首次成功 + 故障失败 + 恢复成功，共 3 次回源耗时记录");
    }

    /** 第一次 getEntry 故意返回空，模拟缓存层瞬时读故障；之后透传给真实实现。 */
    static class FirstReadMissLevel implements CacheLevel<String, String> {

        private final CacheLevel<String, String> delegate;
        private final AtomicInteger calls = new AtomicInteger();

        FirstReadMissLevel(CacheLevel<String, String> delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<CacheEntry<String>> getEntry(String key) {
            if (calls.incrementAndGet() == 1) {
                return Optional.empty();
            }
            return delegate.getEntry(key);
        }

        @Override
        public void putEntry(String key, CacheEntry<String> entry) {
            delegate.putEntry(key, entry);
        }

        @Override
        public void evict(String key) {
            delegate.evict(key);
        }

        @Override
        public void clear() {
            delegate.clear();
        }

        @Override
        public String name() {
            return "first-read-miss-L1";
        }
    }
}
