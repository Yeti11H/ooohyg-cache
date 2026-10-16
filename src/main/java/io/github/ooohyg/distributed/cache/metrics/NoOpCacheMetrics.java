package io.github.ooohyg.distributed.cache.metrics;

import java.time.Duration;

/**
 * 空实现：所有方法 no-op。
 *
 * <p>使用场景：
 *  - 测试（不关心指标时减少噪音）
 *  - 教学（路径更清晰）
 *  - 不需要指标的简单业务
 *
 * <p>为什么接口 + 空实现是标配：
 * 让"不关心指标"成为显式选择，而不是让调用方到处判 null。
 * 编排器永远调 metrics.recordXxx()，metrics 永不为 null。
 */
public class NoOpCacheMetrics implements CacheMetrics {

    @Override public void recordHit(String level) {}
    @Override public void recordMiss(String level) {}
    @Override public void recordNullHit(String level) {}
    @Override public void recordLoadTime(Duration duration) {}
    @Override public void recordLoadFailure(Throwable error) {}
    @Override public void recordSingleFlightMerge() {}
    @Override public void recordInvalidation() {}
    @Override public void recordStaleEventIgnored() {}
}