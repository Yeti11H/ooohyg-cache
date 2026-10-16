package io.github.ooohyg.distributed.cache;

import io.github.ooohyg.distributed.cache.api.DelayedTaskScheduler;
import io.github.ooohyg.distributed.cache.api.SourceWriter;
import io.github.ooohyg.distributed.cache.core.strategy.write.CacheAsideWriteStrategy;
import io.github.ooohyg.distributed.cache.core.strategy.write.DoubleDeleteWriteStrategy;
import io.github.ooohyg.distributed.cache.core.strategy.write.WriteContext;
import io.github.ooohyg.distributed.cache.core.strategy.write.WriteStrategy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 写路径模块测试：CacheAsideWriteStrategy / DoubleDeleteWriteStrategy / DelayedTaskScheduler。
 */
class WriteStrategyModuleTest {

    /** 记录每次调用的假 WriteContext。 */
    static class RecordingWriteContext<K, V> implements WriteContext<K, V> {
        final List<String> calls = new CopyOnWriteArrayList<>();
        final AtomicInteger sourceWrites = new AtomicInteger();
        final AtomicInteger l1Invalidations = new AtomicInteger();
        final AtomicInteger l2Invalidations = new AtomicInteger();
        final List<Long> publishedVersions = new ArrayList<>();
        private final VersionedValue<V> writeResult;

        RecordingWriteContext(VersionedValue<V> writeResult) {
            this.writeResult = writeResult;
        }

        @Override public Duration ttl() { return Duration.ofMinutes(10); }
        @Override public void writeL1(K key, CacheEntry<V> entry) { calls.add("writeL1"); }
        @Override public void writeL2(K key, CacheEntry<V> entry) { calls.add("writeL2"); }
        @Override public void invalidateL1(K key) { calls.add("invalidateL1"); l1Invalidations.incrementAndGet(); }
        @Override public void invalidateL2(K key) { calls.add("invalidateL2"); l2Invalidations.incrementAndGet(); }
        @Override public VersionedValue<V> writeToSource(K key, V value) { calls.add("writeToSource"); sourceWrites.incrementAndGet(); return writeResult; }
        @Override public void publishInvalidation(K key, long version) { calls.add("publish:" + version); publishedVersions.add(version); }
        @Override public void scheduleDelayedInvalidation(K key, long delayMillis) { calls.add("scheduleDelayed"); }
    }

    @Test
    void cacheAside_put_followsWriteSourceThenEvictThenBroadcast() {
        RecordingWriteContext<String, String> ctx =
                new RecordingWriteContext<>(new VersionedValue<>("v", 7L));
        CacheAsideWriteStrategy<String, String> strategy = new CacheAsideWriteStrategy<>();

        strategy.put("k", "value", ctx);

        assertEquals(List.of("writeToSource", "invalidateL2", "invalidateL1", "publish:7"),
                ctx.calls, "Cache Aside 顺序：写源 → 删 L2 → 删 L1 → 广播新版本");
        assertEquals(1, ctx.sourceWrites.get());
        assertEquals(1, ctx.l1Invalidations.get());
        assertEquals(1, ctx.l2Invalidations.get());
    }

    @Test
    void cacheAside_put_nullWriteResult_broadcastsMinusOne() {
        RecordingWriteContext<String, String> ctx = new RecordingWriteContext<>(null);
        new CacheAsideWriteStrategy<String, String>().put("k", "value", ctx);
        assertEquals(-1L, ctx.publishedVersions.get(0), "写源无版本信息时广播 -1（无条件删）");
    }

    @Test
    void cacheAside_invalidate_evictsAndBroadcastsMinusOne() {
        RecordingWriteContext<String, String> ctx =
                new RecordingWriteContext<>(new VersionedValue<>("v", 7L));
        CacheAsideWriteStrategy<String, String> strategy = new CacheAsideWriteStrategy<>();

        strategy.invalidate("k", ctx);

        assertEquals(List.of("invalidateL2", "invalidateL1", "publish:-1"), ctx.calls);
        assertEquals(0, ctx.sourceWrites.get(), "invalidate 不应写数据源");
    }

    @Test
    void cacheAside_put_sourceFailure_wrapsAndThrows() {
        WriteContext<String, String> failing = new RecordingWriteContext<String, String>(
                new VersionedValue<>("v", 7L)) {
            @Override
            public VersionedValue<String> writeToSource(String key, String value) {
                throw new IllegalStateException("db down");
            }
        };
        CacheAsideWriteStrategy<String, String> strategy = new CacheAsideWriteStrategy<>();
        assertThrows(IllegalStateException.class, () -> strategy.put("k", "value", failing),
                "写源失败应包装抛出，不写缓存");
    }

    @Test
    void doubleDelete_performsSecondDeleteAfterDelay() throws Exception {
        RecordingWriteContext<String, String> ctx =
                new RecordingWriteContext<>(new VersionedValue<>("v", 7L));
        DoubleDeleteWriteStrategy<String, String> strategy =
                new DoubleDeleteWriteStrategy<>(new CacheAsideWriteStrategy<>(), Duration.ofMillis(200));

        strategy.put("k", "value", ctx);

        // 第一次立即完成
        assertEquals(1, ctx.l1Invalidations.get());
        assertEquals(1, ctx.l2Invalidations.get());
        assertTrue(strategy.pendingDelayedTasks() >= 1, "延迟任务应被调度");

        // 等待延迟任务执行
        Thread.sleep(600);
        assertEquals(0, strategy.pendingDelayedTasks(), "延迟任务执行后应清空");
        assertEquals(2, ctx.l1Invalidations.get(), "延迟后应再删一次 L1");
        assertEquals(2, ctx.l2Invalidations.get(), "延迟后应再删一次 L2");
        assertEquals(List.of(7L, -1L), ctx.publishedVersions, "第二次广播应是无版本（-1）");
        strategy.shutdown();
    }

    @Test
    void doubleDelete_debouncesRapidWrites_sameKeyOnlyLastExecutes() throws Exception {
        RecordingWriteContext<String, String> ctx =
                new RecordingWriteContext<>(new VersionedValue<>("v", 7L));
        DoubleDeleteWriteStrategy<String, String> strategy =
                new DoubleDeleteWriteStrategy<>(new CacheAsideWriteStrategy<>(), Duration.ofMillis(200));

        // 连续写 3 次：每次 put 立即执行第一次删（3 次），
        // 延迟删经 debounce 只执行最后一次（+1 次）
        strategy.put("k", "a", ctx);
        strategy.put("k", "b", ctx);
        strategy.put("k", "c", ctx);
        Thread.sleep(700);

        assertEquals(4, ctx.l1Invalidations.get(), "3 次即时删 + 1 次延迟删");
        assertEquals(4, ctx.l2Invalidations.get(), "3 次即时删 + 1 次延迟删");
        assertEquals(4, ctx.publishedVersions.size(), "3 次即时广播 + 1 次延迟广播");
        assertEquals(0, strategy.pendingDelayedTasks());
        strategy.shutdown();
    }

    @Test
    void delayedTaskScheduler_debouncesSameKey() throws Exception {
        DelayedTaskScheduler<String> scheduler = new DelayedTaskScheduler<>(Duration.ofMillis(150));
        AtomicInteger executions = new AtomicInteger();

        scheduler.schedule("k", executions::incrementAndGet);
        scheduler.schedule("k", executions::incrementAndGet);
        scheduler.schedule("k", executions::incrementAndGet);

        assertEquals(1, scheduler.pendingCount(), "同 key 应只保留最后一个任务");
        Thread.sleep(500);
        assertEquals(1, executions.get(), "debounce：只执行最后一次");
        assertEquals(0, scheduler.pendingCount());
        scheduler.shutdown();
    }

    @Test
    void delayedTaskScheduler_differentKeys_keepSeparate() throws Exception {
        DelayedTaskScheduler<String> scheduler = new DelayedTaskScheduler<>(Duration.ofMillis(150));
        AtomicInteger executions = new AtomicInteger();

        scheduler.schedule("a", executions::incrementAndGet);
        scheduler.schedule("b", executions::incrementAndGet);
        assertEquals(2, scheduler.pendingCount(), "不同 key 任务互不覆盖");

        Thread.sleep(500);
        assertEquals(2, executions.get());
        scheduler.shutdown();
    }
}
