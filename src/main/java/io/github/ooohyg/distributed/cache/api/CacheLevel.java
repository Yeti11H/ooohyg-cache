package io.github.ooohyg.distributed.cache.api;

import io.github.ooohyg.distributed.cache.CacheEntry;

import java.util.Optional;

/**
 * 单层缓存抽象。L1 / L2 实现它，只负责"存、取、删"。
 *
 * <p>从 MultiLevelCache 中独立出来，让本地实现（如 InMemoryL1Cache）
 * 只依赖接口，不依赖编排器。
 */
public interface CacheLevel<K, V> {

    Optional<CacheEntry<V>> getEntry(K key);

    void putEntry(K key, CacheEntry<V> entry);

    void evict(K key);

    void clear();

    String name();
}