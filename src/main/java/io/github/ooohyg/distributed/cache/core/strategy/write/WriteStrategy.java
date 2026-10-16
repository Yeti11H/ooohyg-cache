package io.github.ooohyg.distributed.cache.core.strategy.write;

/**
 * 写策略。决定“先更新数据源还是先删缓存、删 L1/L2 顺序、是否延迟双删、是否广播”。
 *
 * @param <K> key 类型
 * @param <V> value 类型
 */
public interface WriteStrategy<K, V> {

    /**
     * 业务写入（put 语义=业务写入，具体顺序由策略决定）。
     */
    void put(K key, V value, WriteContext<K, V> context);

    /**
     * 使缓存失效。
     */
    void invalidate(K key, WriteContext<K, V> context);
}
