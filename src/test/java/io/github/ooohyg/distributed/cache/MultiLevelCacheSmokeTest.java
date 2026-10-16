package io.github.ooohyg.distributed.cache;

import io.github.ooohyg.distributed.cache.L1.ConcurrentHashMapL1Cache;
import io.github.ooohyg.distributed.cache.L2.InMemoryL2Cache;
import io.github.ooohyg.distributed.cache.api.CacheInvalidationEvent;
import io.github.ooohyg.distributed.cache.api.CacheLevel;
import io.github.ooohyg.distributed.cache.api.SourceReader;
import io.github.ooohyg.distributed.cache.api.SourceWriter;
import io.github.ooohyg.distributed.cache.core.MultiLevelCache;
import io.github.ooohyg.distributed.cache.core.strategy.read.CacheAsideReadStrategy;
import io.github.ooohyg.distributed.cache.core.strategy.write.CacheAsideWriteStrategy;
import io.github.ooohyg.distributed.cache.invalidation.CacheInvalidationBus;
import io.github.ooohyg.distributed.cache.invalidation.CacheInvalidationSubscriber;
import io.github.ooohyg.distributed.cache.metrics.NoOpCacheMetrics;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 整体流程冒烟测试：验证
 * 1) 读路径：L1 -> L2 -> L3(SourceReader) -> 回填 L2 + L1；
 * 2) Cache Aside 写路径：先写数据源、后删 L1/L2、再广播失效；
 * 3) 失效广播：本实例收到事件后本地缓存失效，下次读取回源；
 * 4) 版本校验：旧版本事件不覆盖/不删除新版本缓存；
 * 5) 事件幂等：重复 eventId 只处理一次。
 */
class MultiLevelCacheSmokeTest {

    /** 内存总线：既能发布又能订阅（MultiLevelCache 会自动把自己注册为订阅者）。 */
    static class InMemoryInvalidationBus implements CacheInvalidationBus {
        private final List<CacheInvalidationSubscriber> subscribers = new CopyOnWriteArrayList<>();
        private final List<CacheInvalidationEvent> published = new CopyOnWriteArrayList<>();

        @Override
        public void subscribe(CacheInvalidationSubscriber subscriber) {
            subscribers.add(subscriber);
        }

        @Override
        public void publish(CacheInvalidationEvent event) {
            published.add(event);
            for (CacheInvalidationSubscriber s : subscribers) {
                s.onEvent(event);
            }
        }

        List<CacheInvalidationEvent> published() {
            return published;
        }
    }

    /** 组装结果：缓存实例 + 总线 + 数据源计数器。 */
    static class Rig {
        final MultiLevelCache<String, String> cache;
        final InMemoryInvalidationBus bus;
        final AtomicInteger loads;
        final AtomicInteger writes;

        Rig(MultiLevelCache<String, String> cache, InMemoryInvalidationBus bus,
            AtomicInteger loads, AtomicInteger writes) {
            this.cache = cache;
            this.bus = bus;
            this.loads = loads;
            this.writes = writes;
        }
    }

    private final AtomicLong versionSeq = new AtomicLong(0);

    private Rig newRig() {
        CacheLevel<String, String> l1 = new ConcurrentHashMapL1Cache<>("L1");
        CacheLevel<String, String> l2 = new InMemoryL2Cache<>("L2");
        AtomicInteger loads = new AtomicInteger(0);
        AtomicInteger writes = new AtomicInteger(0);
        InMemoryInvalidationBus bus = new InMemoryInvalidationBus();

        SourceReader<String, String> reader = key -> {
            loads.incrementAndGet();
            if ("missing".equals(key)) {
                return null;
            }
            return new VersionedValue<>("v1-" + key, 1L);
        };
        SourceWriter<String, String> writer = (key, value) -> {
            writes.incrementAndGet();
            return new VersionedValue<>(value, versionSeq.incrementAndGet() + 1);
        };

        MultiLevelCache<String, String> cache = new MultiLevelCache<>(l1, l2, reader, writer,
                new CacheAsideReadStrategy<>(), new CacheAsideWriteStrategy<>(),
                bus, new NoOpCacheMetrics());
        return new Rig(cache, bus, loads, writes);
    }

    @Test
    void readPath_populatesL1AndL2_withSingleSourceLoad() {
        Rig rig = newRig();

        Optional<String> first = rig.cache.get("k1");
        assertTrue(first.isPresent());
        assertEquals("v1-k1", first.get());

        // 第二次读：L1 命中，不再回源
        Optional<String> second = rig.cache.get("k1");
        assertTrue(second.isPresent());
        assertEquals("v1-k1", second.get());
        assertEquals(1, rig.loads.get(), "回源只应发生一次");
    }

    @Test
    void readPath_nullSource_writesNullPlaceholder_andBlocksReRead() {
        Rig rig = newRig();

        Optional<String> result = rig.cache.get("missing");
        assertFalse(result.isPresent());
        // 数据源返回 null 表示"不存在"——写空值占位符到 L1/L2，防穿透
        assertEquals(1, rig.loads.get());

        // 第二次读命中空值占位符，不穿透数据源，仍返回 empty
        assertFalse(rig.cache.get("missing").isPresent());
        assertEquals(1, rig.loads.get(), "空值占位符生效，不应再次回源");
    }

    @Test
    void writePath_cacheAside_evictsBothLevels_andBroadcasts() {
        Rig rig = newRig();

        // 先填充读缓存
        rig.cache.get("k1");

        // Cache Aside 写：写数据源 -> 删 L1/L2 -> 广播失效
        rig.cache.put("k1", "new-value");

        assertEquals(1, rig.writes.get(), "数据源应写入一次");
        assertEquals(1, rig.bus.published().size(), "应广播一次失效事件");
        assertEquals(EventType.INVALIDATE, rig.bus.published().get(0).eventType());
        // 写后缓存已失效，下一次读取回源拿到的是数据源的值（v1-k1，版本 1）
        assertEquals("v1-k1", rig.cache.get("k1").orElse(null));
    }

    @Test
    void invalidationEvent_evictsLocalCache_andReloadsFromSource() {
        // 两个实例共享同一总线：A 写，B 收失效事件
        CacheLevel<String, String> aL1 = new ConcurrentHashMapL1Cache<>("A-L1");
        CacheLevel<String, String> aL2 = new InMemoryL2Cache<>("A-L2");
        CacheLevel<String, String> bL1 = new ConcurrentHashMapL1Cache<>("B-L1");
        CacheLevel<String, String> bL2 = new InMemoryL2Cache<>("B-L2");

        InMemoryInvalidationBus bus = new InMemoryInvalidationBus();
        AtomicInteger loads = new AtomicInteger();

        SourceReader<String, String> reader = key -> {
            loads.incrementAndGet();
            return new VersionedValue<>("db-" + key, 1L);
        };
        SourceWriter<String, String> writer = (key, value) -> new VersionedValue<>(value, 2L);

        MultiLevelCache<String, String> a = new MultiLevelCache<>(aL1, aL2, reader, writer,
                new CacheAsideReadStrategy<>(), new CacheAsideWriteStrategy<>(), bus, new NoOpCacheMetrics());
        MultiLevelCache<String, String> b = new MultiLevelCache<>(bL1, bL2, reader, writer,
                new CacheAsideReadStrategy<>(), new CacheAsideWriteStrategy<>(), bus, new NoOpCacheMetrics());

        // A、B 都读到缓存（同 key SingleFlight 只在单实例内生效，A、B 各自回源）
        assertEquals("db-k1", a.get("k1").orElse(null));
        assertEquals("db-k1", b.get("k1").orElse(null));
        assertEquals(2, loads.get(), "A、B 各回源一次");

        // A 写：广播失效事件，B 应收到并失效本地缓存
        a.put("k1", "updated");
        assertEquals(1, bus.published().size());
        assertEquals(EventType.INVALIDATE, bus.published().get(0).eventType());

        // B 的缓存应已被事件删除，再次读取会回源；A 写时也已自删缓存，同样回源
        assertEquals("db-k1", b.get("k1").orElse(null));
        assertEquals("db-k1", a.get("k1").orElse(null));
        assertEquals(4, loads.get(), "A、B 各自重新回源一次");
    }

    @Test
    void staleEvent_doesNotEvictNewerLocalVersion() {
        Rig rig = newRig();

        // 填充：数据源 version=1
        rig.cache.get("k1");

        // 收到一个版本更旧的事件（version=0 < 本地 1），不应删除本地缓存
        rig.cache.onEvent(new CacheInvalidationEvent("event-stale", "k1", 0L,
                EventType.INVALIDATE, System.currentTimeMillis()));

        assertTrue(rig.cache.get("k1").isPresent(), "旧版本事件不应清除新版本缓存");
        assertEquals(1, rig.loads.get(), "不应因旧事件触发回源");
    }

    @Test
    void duplicateEvent_isDeduplicatedByIdempotency() {
        Rig rig = newRig();

        rig.cache.get("k1");

        // 同一 eventId 重复投递两次，只有第一次生效（第二次被幂等丢弃）
        CacheInvalidationEvent event = new CacheInvalidationEvent("event-dup", "k1", 1L,
                EventType.INVALIDATE, System.currentTimeMillis());
        rig.cache.onEvent(event);
        rig.cache.onEvent(event);

        // 缓存已被删除，读取重新回源；重复事件不会造成第二次删除
        assertEquals("v1-k1", rig.cache.get("k1").orElse(null));
        assertEquals(2, rig.loads.get());
    }
}
