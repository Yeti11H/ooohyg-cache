package io.github.ooohyg.distributed.cache;

import io.github.ooohyg.distributed.cache.L1.ConcurrentHashMapL1Cache;
import io.github.ooohyg.distributed.cache.L2.InMemoryL2Cache;
import io.github.ooohyg.distributed.cache.api.CacheLevel;
import io.github.ooohyg.distributed.cache.api.SourceReader;
import io.github.ooohyg.distributed.cache.api.SourceWriter;
import io.github.ooohyg.distributed.cache.core.MultiLevelCache;
import io.github.ooohyg.distributed.cache.core.SingleFlight;
import io.github.ooohyg.distributed.cache.core.strategy.read.CacheAsideReadStrategy;
import io.github.ooohyg.distributed.cache.core.strategy.write.CacheAsideWriteStrategy;
import io.github.ooohyg.distributed.cache.failure.FailurePolicy;
import io.github.ooohyg.distributed.cache.invalidation.InMemoryInvalidationBus;
import io.github.ooohyg.distributed.cache.metrics.CacheMetrics;
import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 测试基类：把"建缓存、拼装组件"的脏活抽出来。
 *
 * <p>与结论文档示例的差异（以真实源码为准）：
 * <ul>
 *   <li>项目没有 {@code loader.InMemorySourceReader}，数据源由 {@code api.SourceReader}
 *       与 {@code api.SourceWriter} 两个函数式接口定义，因此这里用 {@link FakeSource} 同时实现二者；</li>
 *   <li>项目没有 {@code metrics.SimpleCacheMetrics}，只有 {@code CacheMetrics} + {@code NoOpCacheMetrics}，
 *       因此这里用 {@link CountingMetrics} 记录各指标，用于断言乱序丢弃、回源失败等计数；</li>
 *   <li>{@code Cache} 接口位于根包 {@code io.github.ooohyg.distributed.cache}，不在 api 子包；</li>
 *   <li>{@code CacheLevel} 的方法为 getEntry / putEntry / evict / clear / name。</li>
 * </ul>
 */
abstract class BaseCacheTest {

    protected ConcurrentHashMapL1Cache<String, String> l1;
    protected InMemoryL2Cache<String, String> l2;
    protected FakeSource source;
    protected InMemoryInvalidationBus bus;
    protected CountingMetrics metrics;
    protected SingleFlight<String, String> singleFlight;
    protected MultiLevelCache<String, String> cache;

    @BeforeEach
    void setUp() {
        l1 = new ConcurrentHashMapL1Cache<>("test-L1");
        l2 = new InMemoryL2Cache<>("test-L2");
        source = new FakeSource();
        bus = new InMemoryInvalidationBus();
        metrics = new CountingMetrics();
        singleFlight = new SingleFlight<>();

        // 默认组装：读用带 SingleFlight 的策略，写用默认 Cache Aside
        cache = new MultiLevelCache<>(
                l1, l2,
                source, source,
                new CacheAsideReadStrategy<>(singleFlight),
                new CacheAsideWriteStrategy<>(),
                bus,
                metrics
        );
    }

    /**
     * 用指定的 L1 / L2 / FailurePolicy 组装一个独立编排器（复用当前数据源与总线），
     * 用于需要非默认降级策略的场景。
     */
    protected MultiLevelCache<String, String> newCache(CacheLevel<String, String> l1Level,
                                                       CacheLevel<String, String> l2Level,
                                                       FailurePolicy<String, String> failurePolicy,
                                                       CacheMetrics cacheMetrics) {
        return new MultiLevelCache<>(
                l1Level, l2Level,
                source, source,
                new CacheAsideReadStrategy<>(new SingleFlight<>()),
                new CacheAsideWriteStrategy<>(),
                bus,
                cacheMetrics,
                Duration.ofMinutes(30), null, failurePolicy, Duration.ofMinutes(1),
                key -> key, key -> key);
    }

    // ---------------- 测试替身 ----------------

    /**
     * 内存数据源：同时实现 SourceReader / SourceWriter，并支持回源计数、延迟与故障注入。
     */
    static class FakeSource implements SourceReader<String, String>, SourceWriter<String, String> {

        private final Map<String, String> data = new ConcurrentHashMap<>();
        private final Map<String, Long> versions = new ConcurrentHashMap<>();
        private final AtomicInteger reads = new AtomicInteger();
        private final AtomicInteger writes = new AtomicInteger();
        private final AtomicLong versionSeq = new AtomicLong();
        private volatile RuntimeException failure;
        private volatile long readDelayMillis;

        /** 预置数据（模拟 DB 已有数据），不计入 write 次数。 */
        void put(String key, String value) {
            data.put(key, value);
            versions.put(key, versionSeq.incrementAndGet());
        }

        int readCount() {
            return reads.get();
        }

        void resetReadCount() {
            reads.set(0);
        }

        int writeCount() {
            return writes.get();
        }

        void failWith(RuntimeException error) {
            this.failure = error;
        }

        void recover() {
            this.failure = null;
        }

        void readDelay(long millis) {
            this.readDelayMillis = millis;
        }

        @Override
        public VersionedValue<String> read(String key) throws Exception {
            reads.incrementAndGet();
            if (readDelayMillis > 0L) {
                Thread.sleep(readDelayMillis);
            }
            RuntimeException error = failure;
            if (error != null) {
                throw error;
            }
            String value = data.get(key);
            if (value == null) {
                return null; // 数据源明确"不存在"
            }
            return new VersionedValue<>(value, versions.getOrDefault(key, 0L));
        }

        @Override
        public VersionedValue<String> write(String key, String value) throws Exception {
            writes.incrementAndGet();
            RuntimeException error = failure;
            if (error != null) {
                throw error;
            }
            long version = versionSeq.incrementAndGet();
            data.put(key, value);
            versions.put(key, version);
            return new VersionedValue<>(value, version);
        }
    }

    /**
     * 计数指标实现：CacheMetrics 只有 NoOp 实现，断言需要真实计数。
     */
    static class CountingMetrics implements CacheMetrics {

        private final AtomicInteger hitL1 = new AtomicInteger();
        private final AtomicInteger hitL2 = new AtomicInteger();
        private final AtomicInteger missL1 = new AtomicInteger();
        private final AtomicInteger missL2 = new AtomicInteger();
        private final AtomicInteger nullHits = new AtomicInteger();
        private final AtomicInteger loadFailures = new AtomicInteger();
        private final AtomicInteger singleFlightMerges = new AtomicInteger();
        private final AtomicInteger invalidations = new AtomicInteger();
        private final AtomicInteger staleEventIgnored = new AtomicInteger();
        private final AtomicInteger loadTimes = new AtomicInteger();

        @Override
        public void recordHit(String level) {
            if ("L1".equals(level)) {
                hitL1.incrementAndGet();
            } else if ("L2".equals(level)) {
                hitL2.incrementAndGet();
            }
        }

        @Override
        public void recordMiss(String level) {
            if ("L1".equals(level)) {
                missL1.incrementAndGet();
            } else if ("L2".equals(level)) {
                missL2.incrementAndGet();
            }
        }

        @Override
        public void recordNullHit(String level) {
            nullHits.incrementAndGet();
        }

        @Override
        public void recordLoadTime(Duration duration) {
            loadTimes.incrementAndGet();
        }

        @Override
        public void recordLoadFailure(Throwable error) {
            loadFailures.incrementAndGet();
        }

        @Override
        public void recordSingleFlightMerge() {
            singleFlightMerges.incrementAndGet();
        }

        @Override
        public void recordInvalidation() {
            invalidations.incrementAndGet();
        }

        @Override
        public void recordStaleEventIgnored() {
            staleEventIgnored.incrementAndGet();
        }

        int hitL1Count() {
            return hitL1.get();
        }

        int hitL2Count() {
            return hitL2.get();
        }

        int missL1Count() {
            return missL1.get();
        }

        int missL2Count() {
            return missL2.get();
        }

        int nullHitCount() {
            return nullHits.get();
        }

        int loadFailureCount() {
            return loadFailures.get();
        }

        int invalidationCount() {
            return invalidations.get();
        }

        int staleEventIgnoredCount() {
            return staleEventIgnored.get();
        }

        int loadTimeCount() {
            return loadTimes.get();
        }
    }
}
