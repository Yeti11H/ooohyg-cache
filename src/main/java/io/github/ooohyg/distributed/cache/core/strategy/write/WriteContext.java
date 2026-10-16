package io.github.ooohyg.distributed.cache.core.strategy.write;

import io.github.ooohyg.distributed.cache.CacheEntry;
import io.github.ooohyg.distributed.cache.VersionedValue;

import java.time.Duration;

/**
 * 写上下文。暴露编排器（MultiLevelCache）提供的写原语，供 WriteStrategy 编排调用顺序。
 */
public interface WriteContext<K, V> {

    /**
     * 业务本次写入指定的 TTL。
     * 供"写时更新缓存"的策略（如 WriteThrough）使用。
     */
    Duration ttl();

    void writeL1(K key, CacheEntry<V> entry);

    void writeL2(K key, CacheEntry<V> entry);

    void invalidateL1(K key);

    void invalidateL2(K key);

    VersionedValue<V> writeToSource(K key, V value) throws Exception;

    void publishInvalidation(K key, long version);

    void scheduleDelayedInvalidation(K key, long delayMillis);
}