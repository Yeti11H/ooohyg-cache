package io.github.ooohyg.distributed.cache.api;

import io.github.ooohyg.distributed.cache.CacheEntry;
import io.github.ooohyg.distributed.cache.VersionedValue;

import java.time.Duration;
import java.util.Optional;

/**
 * 读上下文。暴露编排器（MultiLevelCache）提供的原语，供 ReadStrategy 编排调用顺序。
 */
public interface ReadContext<K, V> {

    /**
     * 默认 TTL——从数据源加载并回填缓存时使用。
     * 由 MultiLevelCache 构造时配置，防雪崩 guard 会在其上叠加抖动。
     */
    Duration defaultTtl();

    /**
     * 空值占位符的 TTL。
     * L3 明确返回"不存在"时用它构造空值 entry——比正常 TTL 短很多。
     */
    Duration nullValueTtl();

    Optional<CacheEntry<V>> readL1(K key);

    Optional<CacheEntry<V>> readL2(K key);

    /**
     * 读旧值——不做版本检查，返回可能过期的 entry。
     * 给 FailurePolicy 返旧值用。
     */
    Optional<CacheEntry<V>> peekStale(K key);

    void writeL1(K key, CacheEntry<V> entry);

    void writeL2(K key, CacheEntry<V> entry);

    VersionedValue<V> loadFromSource(K key) throws Exception;

    /**
     * 处理 L3 加载失败。
     * 编排器内部：
     *   1. 先调 FailurePolicy.onLoadFailure 要降级值；
     *   2. 如果 FailurePolicy 拒绝，用 peekStale 拿旧值。
     *
     * @return 降级结果；empty 表示无法降级
     */
    Optional<CacheEntry<V>> handleLoadFailure(K key, Throwable error);

    void recordHit(String level);

    void recordMiss(String level);

    void recordLoadTime(Duration duration);
}