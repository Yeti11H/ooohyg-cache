package io.github.ooohyg.distributed.cache.metrics;

import java.time.Duration;

/**
 * 缓存指标接口。编排器发生事件时调用这些方法，
 * 具体怎么记（内存计数器 / Micrometer / 日志）由实现类决定。
 *
 * <p>命名约定：
 * <ul>
 *   <li>level 参数是层名，如 "L1"、"L2"、"L3"，不是具体实现类名。
 *       因为指标应该按"层级"聚合，而不是按"实现"聚合。
 *       （今天 L1 是 Caffeine，明天换成别的，指标不应该断崖）</li>
 *   <li>为什么用 {@link Duration} 而不是 long 毫秒：
 *       语义更清晰，避免单位混淆（是毫秒还是纳秒？）。
 *       实现类需要毫秒时调 {@code duration.toMillis()}。</li>
 * </ul>
 */
public interface CacheMetrics {

    /**
     * 记录一次命中。
     *
     * @param level 层级："L1"、"L2"、"L3"
     */
    void recordHit(String level);

    /**
     * 记录一次未命中。
     *
     * @param level 层级："L1"、"L2"、"L3"
     */
    void recordMiss(String level);

    /**
     * 记录一次空值命中（防穿透效果）。
     *
     * @param level 层级
     */
    void recordNullHit(String level);

    /**
     * 记录一次 L3 回源耗时。
     *
     * @param duration 耗时
     */
    void recordLoadTime(Duration duration);

    /**
     * 记录一次 L3 回源失败。
     *
     * @param error 失败异常
     */
    void recordLoadFailure(Throwable error);

    /**
     * 记录一次 SingleFlight 合并（第六阶段使用）。
     */
    void recordSingleFlightMerge();

    /**
     * 记录一次失效事件处理成功。
     */
    void recordInvalidation();

    /**
     * 记录一次因版本过旧被忽略的失效事件。
     */
    void recordStaleEventIgnored();
}