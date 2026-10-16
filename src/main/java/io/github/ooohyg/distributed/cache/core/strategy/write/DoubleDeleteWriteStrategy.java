package io.github.ooohyg.distributed.cache.core.strategy.write;

import io.github.ooohyg.distributed.cache.api.DelayedTaskScheduler;

import java.time.Duration;

/**
 * 延迟双删写策略：CacheAsideWriteStrategy 的增强版。
 *
 * <p>解决的问题：标准 Cache Aside 有一个经典漏洞——
 * 读线程在写线程删缓存之前读到旧值，在写线程删缓存之后才回填旧值，
 * 导致缓存里长期是旧值。
 *
 * <p>做法：在标准流程（写 DB → 删 L2 → 删 L1 → 广播）之后，
 * 延迟 N 毫秒再删一次。这样"延迟窗口内回填的旧值"也会被清掉。
 *
 * <p>debounce：同一 key 短时间多次 put 时，只有最后一次 put 的延迟任务会执行——
 * 因为 DelayedTaskScheduler 内部会取消同 key 的旧任务。
 *
 * <p>组合而非继承：
 * 本类组合一个 {@link WriteStrategy}（通常是 CacheAsideWriteStrategy），
 * 复用它的第一次删逻辑。新增的只是"延迟再删一次"。
 *
 * <p>代价与局限（面试必答）：
 * <ul>
 *   <li>延迟时间无法精确——只能拍一个值，太长影响吞吐，太短防不住；</li>
 *   <li>JVM 重启会丢延迟任务——但缓存也没了，重新读 DB 会建立新缓存；</li>
 *   <li>第二次删也可能失败——需要 Outbox 或定时校对兜底；</li>
 *   <li>不是强一致——只把不一致窗口从"无限期"缩到"延迟期间"。</li>
 * </ul>
 *
 * @param <K> key 类型
 * @param <V> value 类型
 */
public class DoubleDeleteWriteStrategy<K, V> implements WriteStrategy<K, V> {

    private static final Duration DEFAULT_DELAY = Duration.ofMillis(500);

    private final WriteStrategy<K, V> delegate;
    private final DelayedTaskScheduler<K> scheduler;

    public DoubleDeleteWriteStrategy(WriteStrategy<K, V> delegate) {
        this(delegate, DEFAULT_DELAY);
    }

    public DoubleDeleteWriteStrategy(WriteStrategy<K, V> delegate, Duration delay) {
        this.delegate = delegate;
        this.scheduler = new DelayedTaskScheduler<>(delay);
    }

    @Override
    public void put(K key, V value, WriteContext<K, V> context) {
        // 第一次：走标准 Cache Aside 流程
        delegate.put(key, value, context);

        // 第二次：延迟后再删一次，兜住延迟窗口内回填的旧值
        scheduler.schedule(key, () -> {
            context.invalidateL2(key);
            context.invalidateL1(key);
            // version = -1 表示"无版本信息，无条件删"——
            // 延迟双删的目的就是不加判断地删一次，所以不传 version
            context.publishInvalidation(key, -1L);
        });
    }

    @Override
    public void invalidate(K key, WriteContext<K, V> context) {
        // invalidate 不需要延迟双删——它没有"并发回填旧值"的问题
        delegate.invalidate(key, context);
    }

    /**
     * 关闭延迟调度器。应用关闭时调用。
     */
    public void shutdown() {
        scheduler.shutdown();
    }

    /**
     * 当前待执行的延迟任务数（用于测试断言和监控）。
     */
    public int pendingDelayedTasks() {
        return scheduler.pendingCount();
    }
}